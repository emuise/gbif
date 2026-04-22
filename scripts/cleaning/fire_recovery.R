library(tidyverse)
library(dataone)
# windows workaround for an error in dataone as of april 14, 2024
# write('CURL_SSL_BACKEND=openssl', file = "~/.Renviron", append = TRUE)
library(terra)
library(tidyterra)
library(taxize)
data_loc <- here::here("data", "raw_data", "dataone", "fire_recovery")
fs::dir_create(data_loc)

cn <- CNode("PROD")
# we know the specific title we are looking for
queryParams <- list(
  q = "title:Recovery of lodgepole pine forests following mountain pine beetle attack with and without prescribed burning"
)
result <- dataone::query(
  cn,
  solrQuery = queryParams,
  as = "data.frame",
  parse = FALSE
)
# there are 6 results, we want the most recently modified one
recent <- result %>% slice_max(dateModified)

# this PID is just for metadata
pid <- recent %>% pull(identifier)

# look at the resourcemap to query every file assocaited with that PID
resource_map <- recent %>% pull(resourceMap)

pkg_query <- pkg_query <- list(
  q = paste0('resourceMap:"', resource_map, '"'),
  fl = "identifier,fileName,formatId,formatType",
  rows = 100
)

pkg_results <- dataone::query(cn, solrQuery = pkg_query, as = "data.frame")

# get all assocaited ids to download
data_ids <- pkg_results %>%
  # filter(formatType == "DATA") %>%
  pull(identifier)

locations <- dataone::resolve(cn, pid[[1]])
mnId <- locations$data[1, "nodeIdentifier"]
mn <- getMNode(cn, mnId)

# download all files
map(data_ids, \(x) {
  obj <- getObject(mn, x)
  meta <- getSystemMetadata(mn, x)
  filename <- meta@fileName
  savename <- here::here(data_loc, filename)
  if (file.exists(savename)) {
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
  filter(!is.na(value), value > 0) %>%
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
  rename(vernacular = species) %>%
  left_join(regen_species_join, by = c("vernacular" = "query"))
# still needs to be a little cleaned up to darwin core or bqc

# vegetation
veg <- read_csv(here::here(data_loc, "Vegetation.csv")) %>%
  filter(location != "Jasper_Henry")

# i believe these numbers are percent cover, so we can only realistically translate them into occurances
veg_long <- veg %>%
  pivot_longer(Abies_lasiocarpa:Viola_sp.) %>%
  select(location:year, name, value) %>%
  filter(!is.na(value), value > 0) %>%
  # cf means uncertain at genus or species level. here, i remove uncertain genus
  filter(
    !str_starts(name, "cf"),
    name != "leafy_moss_spp.",
    !str_detect(name, "unidentified")
  )

veg_species <- veg_long %>%
  pull(name) %>%
  unique()

# because cladina should currently be cladonia, i am manually making a new query
# while also keeping the original name intact to region back
# based on veg_species
veg_species_join <- tibble(veg_species = sort(veg_species)) %>%
  mutate(
    query_name = str_replace(veg_species, "Cladina", "Cladonia"),
    # cf means uncertain at genus or species level. here, i replace the uncertain species with their genus
    query_name = str_replace(query_name, "_cf.*", "_sp.")
  ) %>%
  mutate(
    taxa = map(query_name, \(x) {
      # i have once again confirmed rows = 1 is fine and fixed all errors (cladina, cf, spp., unidentified)
      classification(x, db = "gbif", rows = 1) %>%
        rbind() %>%
        select(-id) %>%
        pivot_wider(names_from = rank, values_from = name)
    })
  ) %>%
  unnest_wider(taxa)

veg_spatial <- sites_wgs %>%
  left_join(veg_long) %>%
  rename(vernacular = name) %>%
  left_join(veg_species_join, by = c("vernacular" = "veg_species"))

schema <- arrow::read_parquet(
  here::here("data", "cleaned", "salamander.parquet")
) %>%
  names()

veg_sub <- veg_spatial %>%
  filter(!is.na(vernacular)) %>%
  select(
    valid_scientific_name = species,
    observation_value = value,
    year_obs = year,
    vernacular,
    kingdom,
    phylum,
    class,
    order,
    family,
    genus
  ) %>%
  mutate(
    observation_type = "percent cover"
  )

regen_sub <- regen_spatial %>%
  filter(!is.na(vernacular)) %>%
  select(
    valid_scientific_name = species,
    observation_value = abundance,
    year_obs = year,
    vernacular,
    kingdom,
    phylum,
    class,
    order,
    family,
    genus
  ) %>%
  mutate(observation_type = "abundance")

all_sub <- c(regen_sub, veg_sub) %>%
  vect()

ctrd <- centroids(all_sub)

wgs_crd <- crds(ctrd) %>%
  as_tibble() %>%
  rename(longitude = x, latitude = y)

merged <- all_sub %>%
  as_tibble() %>%
  bind_cols(wgs_crd) %>%
  mutate(
    effort_sampling_value = NA,
    effort_sampling_unit = NA,
    effort_sampling_method = NA,
    coordinate_uncertainty = 30,
    coordinate_uncertainty_unit = "meter",
    month_obs = NA,
    day_obs = NA,
    time_obs = NA,
    group = "plants",
    observed_rank = ifelse(str_detect(vernacular, "_sp."), "genus", "species"),
    dataset_name = "Recovery of lodgepole pine forests following mountain pine beetle attack with and without prescribed burning",
    dataset_creator = "Dr. Phil Burton",
    dataset_publisher = "Knowledge Network for Biocomplexity (KNB)",
    dataset_url_information = "https://knb.ecoinformatics.org/view/doi%3A10.5063%2FF1ZG6QQB",
    dataset_url_download = "https://knb.ecoinformatics.org/knb/d1/mn/v2/packages/application%2Fbagit-1.0/urn%3Auuid%3A1ef60533-22b2-419d-a1c5-a2b2cfe50a34",
    dataset_doi = "10.5063/F1ZG6QQB",
    license = "CC-BY-4.0"
  ) %>%
  select(all_of(schema))

save_loc <- here::here(
  "data",
  "cleaned",
  "fire_recovery.parquet")
fs::dir_create(dirname(save_loc))

arrow::write_parquet(merged, save_loc)
