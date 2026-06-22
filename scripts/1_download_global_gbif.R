library(gbifdb)
library(arrow)
library(tidyverse)
library(countrycode)
library(geodata)
library(tidyterra)

loc <- here::here("data", "gbif_global")

# gbif_download(dir = loc)

# get north american countries
noram <- vect("https://github.com/gbif/continents/raw/master/continent_cookie_cutter.gpkg") %>%
  filter(continent_part == "north_america_east") # west is the aleutian islands, not a huge deal to cut them

world <- geodata::world(path = here::here("data", "shps")) %>%
  project(noram)

na_countries <- intersect(world, noram)

na_bbox <- ext(na_countries)

# north american country codes, also converted to the proper gbif format
na_cc <- na_countries %>% pull(GID_0)

na_cc_iso2c <- countrycode(na_cc, origin = "iso3c", destination = "iso2c")

drs <- fs::dir_ls(loc, recurse = T, type = "dir")

link <- drs[length(drs)] %>%
  open_dataset()

ymin <- na_bbox$ymin
ymax <- na_bbox$ymax
xmin <- na_bbox$xmin
xmax <- na_bbox$xmax

na_gbif_db <- drs[length(drs)] %>%
  open_dataset() %>%
  to_duckdb() %>%
  filter(
    occurrencestatus == "PRESENT",
    !basisofrecord %in% c("FOSSIL_SPECIMEN", "LIVING_SPECIMEN"),
    !is.na(species),
    !is.na(decimallatitude), 
    !is.na(decimallongitude),
    countrycode %in% na_cc_iso2c | is.na(countrycode),
    decimallatitude < ymax, decimallatitude > ymin,
    decimallongitude < xmax, decimallongitude > xmin
  )

unique_issues <- na_gbif_db %>%
  mutate(flat_issue = sql("UNNEST(issue).array_element")) %>%
  distinct(flat_issue) %>%
  collect()

geo_issues <- c(
  "COORDINATE_REPROJECTION_FAILED", "COORDINATE_REPROJECTION_SUSPICIOUS",
  "COORDINATE_UNCERTAINTY_METERS_INVALID", "PRESUMED_NEGATED_LATITUDE",
  "FOOTPRINT_WKT_MISMATCH", "FOOTPRINT_WKT_INVALID",
  "COUNTRY_COORDINATE_MISMATCH", "COORDINATE_PRECISION_INVALID",
  "PRESUMED_NEGATED_LONGITUDE", "CONTINENT_COUNTRY_MISMATCH",
  "CONTINENT_COORDINATE_MISMATCH", "PRESUMED_SWAPPED_COORDINATE"
)

# geospatial filtering based on the above 12 issues.
# i am going to look through the issues again to find if i want to include any more
# it is only filtering out about ~7 million points, which is simultaneously a lot and also not a lot
# a lot in that it is 7 million observations gone, not a lot in that there are ~1.5 billion observations
na_gbif_filtered <- na_gbif_db %>%
  filter(
    # Check that the filtered list size is 0 (meaning no blacklisted elements matched)
    sql(sprintf(
      "list_transform(list_filter(issue, x -> x.array_element IN %s), y -> y.array_element) = []",
      # Converts the R character vector into a standard SQL ('A', 'B') string
      paste0("(", paste(sprintf("'%s'", geo_issues), collapse = ", "), ")")
    ))
  )

  count(na_gbif_filtered)