library(tidyverse)
library(dataone)
library(terra)
library(tidyterra)
library(taxize)
data_loc <- here::here("data", "dataone")
fs::dir_create(data_loc)

cn <- CNode("PROD")
# we know the specific title we are looking for
queryParams <- list(q="title:Recovery of lodgepole pine forests following mountain pine beetle attack with and without prescribed burning") 
result <- dataone::query(cn, solrQuery=queryParams, as="data.frame", parse=FALSE)
# there are 6 results, we want the most recently modified one
recent <- result %>% slice_max(dateModified)

# this PID is just for metadata
pid <- recent %>% pull(identifier)

# look at the resourcemap to query every file assocaited with that PID
resource_map <- recent %>% pull(resourceMap)

pkg_query <- pkg_query <- list(q = paste0('resourceMap:"', resource_map, '"'),
                  fl = "identifier,fileName,formatId,formatType",
                  rows = 100)

pkg_results <- dataone::query(cn, solrQuery=pkg_query, as="data.frame")

# get all assocaited ids to download
data_ids <- pkg_results %>% 
  # filter(formatType == "DATA") %>%
  pull(identifier)

locations <- resolve(cn, pid[[1]])
mnId <- locations$data[1, "nodeIdentifier"]
mn <- getMNode(cn, mnId)

# download all files
map(data_ids, \(x) {
  obj <- getObject(mn, x)
  meta <- getSystemMetadata(mn, x)
  filename <- meta@fileName
  savename <- here::here(data_loc, filename)
  if(file.exists(savename)) {
    return(savename)
  }

  writeBin(obj, savename)

  return(savename)
})

# environment_regeneration has the geospatial information
sites <- read_csv(here::here(data_loc, "environment_regeneration.csv"))

sites_wgs <- sites %>%
  select(location:northing) %>%
  filter(location != "Jasper_Henry") %>% # this is in alberta
  group_by(UTM_zone) %>% 
  group_split() %>%
  map2(.x = ., .y = c("epsg:32610", "epsg:32611"), \(x, y) {
    vect(x, geom = c("easting", "northing"), crs = y) %>%
      project("epsg:4326")
  }) %>%
  vect() %>%
  select(-UTM_zone)

# regen file, these are little saplings

regen <- read_csv(here::here(data_loc, "Regeneration.csv")) %>%
  filter(location != "Jasper_Henry")

regen_long <- regen %>%
  select(-vig) %>%
  pivot_longer(h01:d3) %>%
  filter(!is.na(value),
value > 0) %>%
  group_by(location, plot_ID, subplot_ID, plot_radius, year, species) %>%
  summarize(abundance = sum(value))

regen_species <- regen_long %>%
  pull(species) %>%
  unique()

regen_species_join <- classification(regen_species, db = "gbif", rows = 1) %>%
  rbind() %>%
  select(-id) %>%
  pivot_wider(names_from = rank, values_from = name)

regen_spatial <- sites_wgs %>%
  left_join(regen_long) %>%
  left_join(regen_species_join, by = c("species" = "query")) 
# still needs to be a little cleaned up to darwin core or bqc

# vegetation
veg <- read_csv(here::here(data_loc, "Vegetation.csv")) %>%
  filter(location != "Jasper_Henry")

# i believe these numbers are percent cover, so we can only realistically translate them into occurances
veg_long <- veg %>%
  pivot_longer(Abies_lasiocarpa:Viola_sp.) %>%
  select(location:year, name, value) %>%
  filter(!is.na(value),
value > 0)

veg_species <- veg_long %>%
  pull(name) %>%
  unique()
# cladina mitis is strange here
veg_species_join <- classification(veg_species, db = "gbif") %>%
  rbind() %>%
  select(-id) %>%
  pivot_wider(names_from = rank, values_from = name)
