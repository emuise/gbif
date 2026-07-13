library(gbifdb)
library(arrow)
library(tidyverse)
library(countrycode)
library(geodata)
library(terra)
library(tidyterra)
library(sf) # fast intersect
library(geos) # faster intersect

loc <- here::here("data", "gbif_global")

## download ALL gbif data
# pak::pak("minioclient")
# minioclient::install_mc()
# rerun download code until it is done
# gbifdb::gbif_download()

## filter gbif data to north america
# get north american countries
noram <- vect(
  "https://github.com/gbif/continents/raw/master/continent_cookie_cutter.gpkg"
) %>%
  filter(continent_part == "north_america_east") # west is the aleutian islands, not a huge deal to cut them

world <- geodata::world(path = here::here("data", "shps")) %>%
  project(noram)

na_countries <- intersect(world, noram)

na_bbox <- ext(na_countries)

# north american country codes, also converted to the proper gbif format
na_cc <- na_countries %>% pull(GID_0)

na_cc_iso2c <- countrycode(na_cc, origin = "iso3c", destination = "iso2c")

ymin <- na_bbox$ymin
ymax <- na_bbox$ymax
xmin <- na_bbox$xmin
xmax <- na_bbox$xmax

geo_issues <- c(
  "COORDINATE_REPROJECTION_FAILED",
  "COORDINATE_REPROJECTION_SUSPICIOUS",
  "COORDINATE_UNCERTAINTY_METERS_INVALID",
  "PRESUMED_NEGATED_LATITUDE",
  "FOOTPRINT_WKT_MISMATCH",
  "FOOTPRINT_WKT_INVALID",
  "COUNTRY_COORDINATE_MISMATCH",
  "COORDINATE_PRECISION_INVALID",
  "PRESUMED_NEGATED_LONGITUDE",
  "CONTINENT_COUNTRY_MISMATCH",
  "CONTINENT_COORDINATE_MISMATCH",
  "PRESUMED_SWAPPED_COORDINATE"
)

scratch_dir <- here::here("scratch", "noram_process")

fs::dir_create(scratch_dir)
clean_dir <- here::here("data", "gbif_noram")
fs::dir_create(clean_dir)

dones <- fs::dir_ls(scratch_dir)

files <- fs::dir_ls(loc, recurse = T, type = "file")

files_left <- files[!(basename(files) %in% basename(dones))]

walk(files_left, \(file) {
  # this tempfile allows us to restart where we left off
  tempfile <- here::here("scratch", "noram_process", basename(file))
  if (fs::file_exists(tempfile)) {
    return()
  }
  message(paste0("Processing file ", file, " of ", length(files), "..."))

  filtered_df <- file %>%
    open_dataset() %>%
    filter(
      occurrencestatus == "PRESENT",
      !basisofrecord %in% c("FOSSIL_SPECIMEN", "LIVING_SPECIMEN"),
      !is.na(species),
      !is.na(decimallatitude),
      !is.na(decimallongitude),
      countrycode %in% na_cc_iso2c | is.na(countrycode),
      decimallatitude < ymax,
      decimallatitude > ymin,
      decimallongitude < xmax,
      decimallongitude > xmin
    ) %>%
    collect()

  filtered_df <- filtered_df %>%
    filter(!purrr::map_lgl(issue, ~ any(.x$array_element %in% geo_issues)))

  if (nrow(filtered_df) > 0) {
    write_dataset(
      filtered_df,
      path = clean_dir,
      format = "parquet",
      partitioning = c("kingdom", "family"),
      existing_data_behavior = "overwrite",
      max_partitions = 20000,
      basename_template = paste0("part-", basename(file), "-{i}.parquet")
    )
  }
  # save the tempfile
  fs::dir_create(dirname(tempfile))
  fs::file_create(tempfile)

  rm(filtered_df)
  gc(full = TRUE)
  gc(full = TRUE)
})

condense_fold_name <- "gbif_noram2"

condensed_dir <- here::here("data", condense_fold_name)


l1 <- fs::dir_ls(clean_dir, recurse = F, type = "dir")

# condense the parquet files so they arent overly fragmented
walk(l1, \(x) {
  l2 <- fs::dir_ls(x, type = "dir")

  walk(l2, \(y) {
    dirname <- str_replace(y, "gbif_noram", condense_fold_name)
    if (fs::dir_exists(dirname)) {
      return()
    }
    message(paste0("processing", dirname))

    fs::dir_create(dirname)

    files <- fs::dir_ls(y, type = "file")
    pq <- open_dataset(files)
    write_dataset(
      pq,
      path = dirname,
      format = "parquet",
      existing_data_behavior = "overwrite",
      max_rows_per_file = 1000000,
      max_partitions = 20000
    )

    gc(full = T)
    gc(full = T)
  })
})

# now that the files are condesned, listing them is relaitvely quick
# we actually want to operate on a file bases, and can condense the output at the end
# so basically, birds are going to be unable to process all at once
# i.e. geese have > 100 million observations
# every other family is /reasonably large/
# and can be done at once

# operate on condensed files
c_files <- fs::dir_ls(condensed_dir, recurse = T, type = "file")

# this is the tester parquet file!
# each iterate needs to efficiently work on a 1 million row file
# this is one of them
# bees!
x <- "E:/rprojects/gbif/data/gbif_noram2/kingdom=Animalia/family=Apidae/part-0.parquet"

bcb <- bcmaps::bc_bound_hres() %>%
  vect()

bcb_sf <- bcb %>%
  st_as_sf()

bcb_bbox <- bcb %>%
  project("epsg:4326") %>%
  ext()

spatcount_dir <- here::here("data", "spatial_counts")
fs::dir_create(spatcount_dir)

walk(c_files, \(x) {
  kingdom <- dirname(x) %>% dirname() %>% basename() %>% str_remove("kingdom=")
  family  <- dirname(x) %>% basename() %>% str_remove("family=")
  
  savename <- here::here(spatcount_dir, glue::glue("{kingdom}_{family}_{basename(x)}"))
  if (file.exists(savename)) return()
  
  message(paste("Counting", x))
  
  df <- arrow::open_dataset(x) %>%
    mutate(kingdom = kingdom, family = family) %>%
    select(kingdom, phylum, class, order, family, genus, species,
           decimallatitude, decimallongitude) %>%
    collect()
  # index of if points are in bounding box
  in_bbox <- df$decimallatitude  <= bcb_bbox$ymax &
             df$decimallatitude  >= bcb_bbox$ymin &
             df$decimallongitude <= bcb_bbox$xmax &
             df$decimallongitude >= bcb_bbox$xmin
  
  # static integer column
  df$in_bc <- 0L
  
  # if any are in the bounding box, do a spatial intersect to see if they are in the BC polygon
  if (any(in_bbox)) {

    # sf is fastest for these intersections, keeping the polygon in it's normal crs is also fastest
    # so we project the points from wgs84 into bc albers for speed
    # we dont need to reproject out or anything
    spat <- vect(
      df[in_bbox, ],
      geom = c("decimallongitude", "decimallatitude"),
      crs = "epsg:4326"
    ) %>%
      project(bcb) %>%
      st_as_sf()

    inters <- st_intersects(spat, bcb_sf, sparse = T)

    # if number of intersects greater than 0, it is within the bounding box of bc
    df$in_bc[in_bbox] <- as.integer(lengths(inters) > 0)
  }

  # group by summarize if it is inside or outside bc
  inout <- df %>%
    group_by(kingdom, phylum, class, order, family, genus, species) %>%
    summarize(
      n_bc    = sum(in_bc == 1L),
      n_notbc = sum(in_bc == 0L),
      n_total = n(),
      .groups = "drop"
    )
  
  arrow::write_parquet(inout, savename)
}, .progress = TRUE)