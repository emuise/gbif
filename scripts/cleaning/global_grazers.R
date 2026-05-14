library(tidyverse)
library(dataone)
# windows workaround for an error in dataone as of april 14, 2024
# write('CURL_SSL_BACKEND=openssl', file = "~/.Renviron", append = TRUE)
library(terra)
library(tidyterra)
library(taxize)
data_loc <- here::here("data", "raw_data", "dataone", "global_grazers")
fs::dir_create(data_loc)

cn <- CNode("PROD")
mn <- getMNode(cn, "urn:node:KNB")


# Define the identifier
packageId <- "doi:10.5063/F12F7KVF"

# Download the package
pkg <- getPackage(mn, packageId)

# nicely named ddata
unzip(pkg, exdir = data_loc)

subfold <- here::here(data_loc, "data")

study_data <- read_csv(here::here(subfold, "studyData.csv")) %>%
  filter(Country == "Canada", !is.na(Longitude), !is.na(Latitude)) %>%
  mutate(across(c(Longitude, Latitude), as.numeric))

bcb <- bcmaps::bc_bound_hres() %>% vect()
bcb_wgs <- bcb %>% project("EPSG:4326")
study_spatial <- study_data %>%
  vect(geom = c("Longitude", "Latitude")) %>%
  filter(str_detect(Title, "Vesper", negate = T)) %>% # Vesper has too many disparate sites to be used here
  filter(UniqueID != 1139) %>% # doesn't actually include grazer data 
  intersect(bcb_wgs)

# we are left with one study, which has two sites. these sites actually have full lat lon data
# in the study itself 
# https://faculty.tru.ca/lfraser/schmidt_et_al_2012.pdf
id <- study_spatial %>%
  pull(UniqueID)

# this data is also useless because it has a year range of 1995-2002
grazers <- here::here(subfold, "grazingData.csv") %>%
  read_csv() %>%
  filter(UniqueID == id,
         Estimate == "abundance")
