library(tidyverse)
library(dataone)
# windows workaround for an error in dataone as of april 14, 2024
# write('CURL_SSL_BACKEND=openssl', file = "~/.Renviron", append = TRUE)
library(terra)
library(tidyterra)
library(taxize)

data_loc <- here::here("data", "raw_data", "dataone", "pink_mountain")
fs::dir_create(data_loc)

cn <- CNode("PROD")
# we know the specific title we are looking for
queryParams <- list(
  q = "title:Alpine pollinators unique species list for Pink Mountain, BC, Canada"
)
result <- dataone::query(
  cn,
  solrQuery = queryParams,
  as = "data.frame",
  parse = FALSE
)

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
  filter(formatType == "DATA") %>%
  pull(identifier)

locations <- dataone::resolve(cn, pid[[1]])
mnId <- locations$data[1, "nodeIdentifier"]
mn <- getMNode(cn, mnId)


obj <- getObject(mn, data_ids)
meta <- getSystemMetadata(mn, data_ids)

savename <- here::here(data_loc, "pinkmtn_idlist.csv")

writeBin(obj, savename)

data <- read_csv(savename) %>%
  janitor::clean_names() %>%
  select(latitude = n, longitude = w, day, month, year, genus, species = speceies) %>%
  mutate(vernacular = paste(genus, species),
month = 6)

species_join <- data$vernacular %>%
  unique() %>%
  str_replace("NA", "sp.") %>%
  classification(db = "gbif", rows = 1) %>%
  rbind() %>%
  select(-id) %>%
  pivot_wider(names_from = rank, values_from = name) %>%
  mutate(query = str_replace(query, "sp.", "NA"))


spatial <- data %>%
  select(-genus, -species) %>%
  left_join(species_join, by = c("vernacular" = "query")) %>%
  mutate(latitude = latitude / 2,
    longitude = ifelse(nchar(data$longitude) == 6, longitude, longitude / 10)) %>%
  vect(geom = c("longitude", "latitude"), crs = "epsg:32610")

ggplot() +
  geom_spatvector(data = bcmaps::bc_bound() %>%
    vect()) +
  geom_spatvector(data = spatial)


message("unfortunately the pink mountain species list doesn't have accurate geoinformation")