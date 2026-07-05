library(gbifdb)
library(arrow)
library(tidyverse)
library(countrycode)
library(geodata)
library(tidyterra)

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
  "PRESUMED_SWAPPED_COORDINATE",
  # this is a fix for the taxonomic backbone
  # basically, if the parser cant find a perfect match to the submitted species ID, it may fuzzy match the species,
  # this could lead to a confident GENUS
  "TAXON_MATCH_HIGHERRANK"
)

clean_dir <- here::here("data", "gbif_noram")
fs::dir_create(clean_dir)


files <- fs::dir_ls(loc, recurse = T, type = "file")

map(files, \(file) {
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
  gc()
})
