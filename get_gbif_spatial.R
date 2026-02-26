library(tidyverse)
library(terra)
library(tidyterra)
library(bcmaps)
library(rgbif)
library(arrow)
library(geoarrow)
library(lubridate)
# my personal library for default colours and themes
# remove it if you don't have or want to use it
# install it with
# pak::pak("emuise/muiselib2")
# this needs ibm plex sans from google
# found here
# https://fonts.google.com/specimen/IBM+Plex+Sans
muiselib2::muiselib_setup()

# get broad bcb for the hull to download with
bcb <- bcmaps::bc_bound() %>%
  vect()

# download and load a "well-known-text" for the bounding box of BC
# in the proper coordinate system (WGS84); this is what GBIF uses
bcb_hull <- bcb %>%
  hull() %>%
  project("epsg:4326") %>%
  st_as_sf() %>%
  st_make_valid() %>%
  st_geometry() %>%
  st_as_text()

# get high resolution bc maps boundaries for when we spatialize tha parquet files
bcb_hres <- bcmaps::bc_bound_hres()

# need to set a username and password
# see here for instructions
# https://docs.ropensci.org/rgbif/articles/gbif_credentials.html

# identify recent (within 30 days) downloads that were initaited from RGBIF and not cancelled
all_downloads <- occ_download_list()$results

downloads_subset <- all_downloads %>%
  filter(
    status == "SUCCEEDED",
    source == "rgbif",
    as.Date(created) > (today() - 30)
  )

# for some reason using pred_default() with the pred_within() bricks it
# Construct the query predicate: combines the BC spatial hull with standard quality filters
my_pred <- pred_and(
  pred_within(bcb_hull),
  # the rest of this is just pred_default()
  pred("HAS_GEOSPATIAL_ISSUE", FALSE),
  pred("HAS_COORDINATE", TRUE),
  pred("OCCURRENCE_STATUS", "PRESENT"),
  pred_not(pred_in("BASIS_OF_RECORD", c("FOSSIL_SPECIMEN", "LIVING_SPECIMEN")))
)

# Create a dataframe representation of the standard default predicates for comparison
pred_default_in <- pred_default() %>%
  map(unlist) %>%
  map_dfr(bind_rows)

# Extract the API's internal representation of default predicates from a known reference download.
# This allows us to filter out standard defaults when comparing against the download history.
pred_default_out <- all_downloads %>%
  # using a known download with pred_default() enabled to extract what
  # it looks like in the download list
  filter(key == "0028084-260208012135463") %>%
  pull(request.predicate.predicates) %>%
  as.data.frame() %>%
  as_tibble() %>%
  filter(is.na(geometry)) %>%
  select(-geometry)

# this should work without access to my download list from my file.
# i need to test if i can use this without a login to MY account specifically
# the easiest way to do this is probably by putting it in a package tbh
pred_default_out <- occ_download_meta(
  "0028084-260208012135463"
)$request$predicate$predicates %>%
  map(unlist) %>%
  map_dfr(bind_rows) %>%
  nest(predicate.values = starts_with("predicate.values")) %>%
  mutate(across(ends_with("matchCase"), as.logical)) %>%
  filter(is.na(geometry)) %>%
  select(-geometry) %>%
  # empties the predicate.values rows where the values are all NA
  mutate(
    predicate.values = map(predicate.values, function(x) {
      if (all(is.na(x))) {
        return(NULL)
      } else {
        return(as.character(x))
      }
    })
  )

# Flatten the current custom predicate into a dataframe
my_pred_df <- map(my_pred, unlist) %>%
  map_dfr(bind_rows)

# Identify the non-default components of the current query (e.g., the spatial polygon)
non_default_df <- anti_join(my_pred_df, pred_default_in) %>%
  select(where(~ any(!is.na(.x))))

# Check the recent download history to see if an identical query has already been run.
# This logic compares the custom (non-default) predicates of past downloads against the current one.
exact_downloads <- downloads_subset %>%
  select(key, doi, request.predicate.predicates) %>%
  mutate(
    matched = map_lgl(request.predicate.predicates, \(x) {
      # Remove default predicates from the historical download's criteria
      non_defaults <- anti_join(x, pred_default_out) %>%
        select(where(~ any(!is.na(.x))))

      n_nd <- nrow(non_defaults)

      # Check if the remaining predicates match the current custom predicates
      n_matched <- inner_join(non_defaults, non_default_df) %>%
        nrow()

      if (n_nd == n_matched) {
        return(T)
      }

      return(F)
    })
  ) %>%
  filter(matched)

# get the most recent exactly downloaded key
download_number <- exact_downloads %>%
  head(1) %>%
  pull(key)

message(download_number)

fs::dir_create("data")

# if length of valid_dn is 0, we need to download a new version of GBIF
# as we havent previously downloaded this EXACT request
# or it is stale (> 30 days old)
if (length(download_number) == 0) {
  # check if our predicates work/will work
  prepped <- occ_download_prep(my_pred, format = "SIMPLE_PARQUET")

  # check that the predicate is valid
  # request the files from GBIF: NOTE IF YOU RERUN THIS IT WILL GENERATE
  # A NEW REQUEST
  user_input <- readline(
    "Are you sure you want to generate a new GBIF request? (y/n)"
  )
  if (user_input != "y") {
    stop(
      "User did not want to generate a new GBIF request. Double check predicates if you think there should be a matched download number"
    )
  }

  download_number <- occ_download(my_pred, format = "SIMPLE_PARQUET")

  occ_download_wait(download_number)
}

if (!file.exists(here::here("data", glue::glue("{download_number}.zip")))) {
  # download and unzip the data
  # occ_download_import doesnt seem to work correctly
  # so im doing it manually
  d <- occ_download_get(download_number, path = "data")
  d %>% as.character() %>% unzip(exdir = "data")

  # list all the files in the folder so i can append their filetype (".parquet")
  ins <- fs::dir_ls(here::here("data", "occurrence.parquet"))
  outs <- glue::glue("{ins}.parquet")
  # add filetypes so they are now readable
  walk2(ins, outs, fs::file_move, .progress = T)
}

files <- here::here("data", "occurrence.parquet") %>%
  fs::dir_ls() %>%
  fs::file_info() %>%
  filter(size != 0) %>%
  pull(path)

d <- arrow::open_dataset(files) %>%
  mutate(
    across(
      c(kingdom, phylum, class, order, family, genus, species),
      ~ coalesce(.x, "unknown")
    )
  ) %>%
  group_by(kingdom, phylum, class)


hive_path <- here::here("data", "gbif_hive")

# resave as hive partitioning based on taxononmy, maybe will allow for easier summarization in the future
# commenting out so i can run the full thing at once
# arrow::write_dataset(
#   d,
#   path = hive_path,
#   max_rows_per_file = 50000,
#   existing_data_behavior = "error"
# )

# copy the file directory for the hive to be spatial
# we cant just make the milions of points spatial at once, it crashes computers to do that (3-50 m) points is too many!
fs::dir_ls(hive_path, recurse = T, type = "dir") %>%
  str_replace("gbif_hive", "gbif_hive_spatial") %>%
  fs::dir_create()

fs::dir_ls(hive_path, recurse = T, type = "file") %>%
  map(
    \(x) {
      print(x)
      savename <- str_replace(x, "gbif_hive", "gbif_hive_spatial")
      savename_empty <- str_replace(savename, ".parquet", ".txt")
      if (file.exists(savename) | file.exists(savename_empty)) {
        # message("Already processed this one")
        return()
      }
      pq <- read_parquet(x)

      v <- st_as_sf(
        pq,
        coords = c("decimallongitude", "decimallatitude"),
        crs = 4326
      ) %>%
        st_transform(3005)

      # this is a fast intersect for this use case
      # st_intersection is VERY slow due to now index in R
      out <- v %>%
        mutate(
          intersect = st_intersects(v, bcb_hres) %>%
            as.logical
        ) %>%
        filter(intersect) %>%
        select(-intersect)

      # if there are no valid observations in the file, save a text file so we can skip it next time
      # and pick up from where we left off
      if (nrow(out) == 0) {
        fs::file_create(savename_empty)
        return()
      }

      write_parquet(out, savename)
    },
    .progress = T
  )

hive_path_s <- here::here("data", "gbif_hive_spatial")
# list all non ".txt" files
files_s <- fs::dir_ls(
  hive_path_s,
  recurse = T,
  type = "file",
  regexp = ".parquet$"
)

gbif_spatial <- open_dataset(files_s)