library(gbifdb)
library(arrow)
library(tidyverse)
library(countrycode)
library(geodata)
library(tidyterra)
library(duckdb)

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

drs <- fs::dir_ls(loc, recurse = T, type = "dir")

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

## standard gbif cleaning procedures
na_gbif_arrow <- drs[length(drs)] %>%
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
  )

clean_dir <- here::here("data", "gbif_noram")
fs::dir_create(clean_dir)

con <- dbConnect(duckdb())
dbExecute(con, "SET preserve_insertion_order = false;")
dbExecute(con, "SET threads = 4;") 

duckdb::duckdb_register_arrow(con, "raw_gbif_table", na_gbif_arrow)

dbExecute(con, sprintf("
  CREATE OR REPLACE VIEW test_filtered_view AS 
  SELECT * EXCLUDE (issue) 
  FROM (
    SELECT * FROM raw_gbif_table
  ) AS sampled_table
  WHERE NOT list_has_any(
    list_transform(issue, x -> x.array_element), 
    CAST(%s AS VARCHAR[])
  )
  ORDER BY genus, species;
", paste0("[", paste(sprintf("'%s'", geo_issues), collapse = ", "), "]")))

message("Streaming sample dataset out to disk partitions...")
dbExecute(con, sprintf("
  COPY test_filtered_view 
  TO '%s' 
  (FORMAT 'PARQUET', PARTITION_BY 'genus', OVERWRITE_OR_IGNORE 1);
", clean_dir))

dbDisconnect(con, shutdown = TRUE)