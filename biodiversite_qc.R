install.packages(c('dplyr','duckdb','duckdbfs', 'sf'))
library(dplyr)
library(duckdb)
library(duckdbfs)
library(sf)

source("http://atlas.biodiversite-quebec.ca/bq-atlas-parquet.R")

atlas_dates