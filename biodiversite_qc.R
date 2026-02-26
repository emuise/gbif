# install.packages(c('dplyr','duckdb','duckdbfs', 'sf'))
library(dplyr)
library(duckdb)
library(duckdbfs)
library(sf)

source("http://atlas.biodiversite-quebec.ca/bq-atlas-parquet.R")

atlas_dates

atlas_rem <- atlas_remote(tail(atlas_dates$dates, n = 1))
colnames(atlas_rem)

datasets <- atlas_rem |>
  group_by(dataset_name) |>
  summarize(cnt = count()) |>
  arrange(desc(cnt))

iris_vers <- atlas_rem |>
  filter(valid_scientific_name == 'Iris versicolor') |>
  mutate(geom = ST_Point(as.numeric(longitude), as.numeric(latitude))) |>
  to_sf() |>
  collect()


atlas_rem %>%
  filter(!is.na(geom_bbox)) %>%
  head(5) %>%
  collect() %>%
  select(geom_bbox) %>%
  unnest(geom_bbox) %>%
  pmap(function(xmin, ymin, xmax, ymax, ...) {
    as.polygons(ext(xmin, xmax, ymin, ymax), crs = "EPSG:4326")
  }) %>%
  vect() %>%
  filter(row_number() == 1) %>%
  plot()
