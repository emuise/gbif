library(tidyverse)
library(terra)
library(tidyterra)
library(bcmaps)
library(arrow)
library(geoarrow)
library(duckdb)

# list all of the non-empty files from the gbif
files_aspatial <- here::here("data", "gbif_hive_spatial") %>%
  fs::dir_ls(recurse = T, type = "file") %>%
  str_subset(".parquet$") %>%
  fs::file_info() %>%
  filter(size != 0) %>%
  pull(path)

# this makes the spatial duckdb database and puts all of the parquet files from
# gbif into it
db_loc <- here::here("data", "duckdb", "gbif.duckdb")
fs::dir_create(dirname(db_loc))
# we need to ensure version 1.5.0, this is where the real geospatial capabilities come into play
ddb <- dbConnect(
  duckdb(),
  dbdir = db_loc,
  config = list("storage_compatibility_version" = "v1.5.0")
)

# basically because we have 'large' data, doing it multithreaded is much quicker
dbExecute(ddb, "INSTALL spatial; LOAD spatial")
dbExecute(ddb, "SET threads TO 8;")
dbExecute(ddb, "SET memory_limit = '8GB';")
dbExecute(ddb, "SET preserve_insertion_order = false;")

# add bc geometry to the duckdb to remove points outside the province
bcb_loc <- here::here("data", "shps", "bcb_wgs84.gpkg")

if (!file.exists(bcb_loc)) {
  bc_geom <- bcmaps::bc_bound_hres() %>%
    sf::st_transform(4326) %>%
    sf::st_union() %>%
    st_as_sf() %>%
    vect()

  fs::dir_create(dirname(bcb_loc), recurse = T)

  writeVector(bc_geom, bcb_loc, overwrite = T)
}

dbExecute(
  ddb,
  glue::glue(
    "
  CREATE TABLE IF NOT EXISTS bc_boundary AS 
  SELECT * FROM st_read('{bcb_loc}');
"
  )
)


db_tables <- dbListTables(ddb)

if (!("gbif_bc" %in% db_tables)) {
  # list files in good format for sql
  files_list_sql <- paste0("['", paste(files_aspatial, collapse = "','"), "']")

  # import table via sql, specifying that we are using wgs84 (default of gbif)
  dbExecute(
    ddb,
    glue::glue(
      "
  CREATE TABLE IF NOT EXISTS gbif_raw AS 
  SELECT 
    *, 
    st_setcrs(ST_Point(decimallongitude, decimallatitude), 'EPSG:4326')::GEOMETRY AS geom
  FROM read_parquet({files_list_sql}, hive_partitioning = 1)
  WHERE decimallongitude IS NOT NULL AND decimallatitude IS NOT NULL;
"
    )
  )

  # make spatial index because we are going to process to h3 polygons at some point
  dbExecute(
    ddb,
    "CREATE INDEX IF NOT EXISTS idx_raw_geom ON gbif_raw USING RTREE (geom);"
  )

  # crop to british columbia within the database as new table
  dbExecute(ddb, "
    CREATE TABLE gbif_bc AS 
    SELECT * FROM gbif_raw 
    WHERE ST_Intersects(geom, (SELECT geom FROM bc_boundary LIMIT 1));
  ")

  # save progress
  dbExecute(ddb, "CHECKPOINT;")
}

# install h3 hexagons
dbExecute(ddb, "INSTALL h3 FROM community; LOAD h3;")

res_level <- 6

h3_summary <- dbGetQuery(
  ddb,
  glue::glue(
    "
  SELECT 
    h3_latlng_to_cell(decimallatitude, decimallongitude, {res_level}) AS h3_token,
    count(*) AS occurrence_count,
    h3_cell_to_boundary_wkt(h3_latlng_to_cell(decimallatitude, decimallongitude, {res_level})) AS wkt
  FROM gbif_spatial
  GROUP BY h3_token, wkt
"
  )
)


dbDisconnect(ddb, shutdown = T)
