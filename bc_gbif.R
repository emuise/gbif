library(tidyverse)
library(terra)
library(tidyterra)
library(bcmaps)
library(rgbif)
library(arrow)
library(geoarrow)
# my personal library for default colours and themes
# remove it if you don't have or want to use it
# install it with
# pak::pak("emuise/muiselib2")
muiselib2::muiselib_setup()

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

my_downloads <- occ_download_list()$results

# for some reason using pred_default() with the pred_within() bricks it
my_pred <- pred_and(pred_within(bcb_hull),
   pred("HAS_GEOSPATIAL_ISSUE",FALSE),
   pred("HAS_COORDINATE",TRUE),
   pred("OCCURRENCE_STATUS","PRESENT"),
   pred_not(pred_in("BASIS_OF_RECORD",
    c("FOSSIL_SPECIMEN","LIVING_SPECIMEN")))
  )

prepped <- occ_download_prep(my_pred, format = "SIMPLE_PARQUET")

# check that the predicate is valid
# request the files from GBIF: NOTE IF YOU RERUN THIS IT WILL GENERATE
# A NEW REQUEST
occ_download(my_pred, format = "SIMPLE_PARQUET")

download_number <- "0028084-260208012135463"

# check if this has been downloaded in the last 30 days
occ_download_cached(my_pred, refresh = T)

occ_download_wait(download_number)

fs::dir_create("data")

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

files <- fs::file_info(outs) %>%
  filter(size != 0) %>%
  pull(path)

test <- files[[2]] %>%
  read_parquet()

test %>%
  drop_na(c("decimallongitude", "decimallatitude")) %>%
  filter(coordinateuncertaintyinmeters < 1000) %>%
  st_as_sf(coords = c("decimallongitude", "decimallatitude"), crs = 4326) %>%
  vect() %>%
  project(bcb) %>%
  plot(add = T)

st_as_sf(test, coords = c("decimallongitude", "decimallatitude"))


d <- arrow::open_dataset(files) %>%
  filter(!is.na(decimallongitude),
         !is.na(decimallatitude),
         coordinateuncertaintyinmeters < 1000) %>%
  collect()

d_v <- st_as_sf(d, coords = c("decimallongitude", "decimallatitude")) %>%
  vect()
d %>% 
  count(stateprovince) %>% 
  collect() %>% 
  arrange(desc(n)) %>%
  mutate(cat = fct_lump_n(stateprovince, 10, w = n, other_level = "other")) %>%
  group_by(cat) %>%
  summarize(n = sum(n))
