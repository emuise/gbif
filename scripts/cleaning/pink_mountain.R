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
mn <- getMNode(cn, "urn:node:KNB")


# Define the identifier
packageId <- "doi:10.5063/F14J0CMT"

# Download the package
pkg <- getPackage(mn, packageId)

# unfortunately the data isn't nicely named and there isnt an easy way to attach that info
unzip(pkg, exdir = data_loc)

######
data <- read_csv(here::here(data_loc, "data", "species_list.csv")) %>%
  janitor::clean_names() %>%
  select(latitude = latitude_dd, longitude = longitude_dd, day, month, year, genus, species = species) %>%
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

schema <- arrow::read_parquet(
  here::here("data", "cleaned", "salamander.parquet")
) %>%
  names()


merged <- data %>%
  select(-species, -genus) %>%
  left_join(species_join, by = c("vernacular" = "query")) %>%
  mutate(valid_scientific_name = species,
  observation_value = 1,
observation_type = "checklist",
effort_sampling_value = NA,
effort_sampling_method = NA,
effort_sampling_unit = NA,
coordinate_uncertainty = 100,
coordinate_uncertainty_unit = "meters",
year_obs = year,
month_obs = month,
day_obs = day,
time_obs = NA,
group = "Insects",
observed_rank = ifelse(is.na(species), "genus", "species"),
dataset_name = "Alpine pollinators unique species list for Pink Mountain, BC, Canada christopher lortie and Anya Reid",
dataset_creator = "Christopher Lortie",
dataset_publisher = "Knowledge Network for Biodiversity",
dataset_url_information = "https://knb.ecoinformatics.org/view/doi:10.5063/F14J0CMT",
dataset_url_download = "https://knb.ecoinformatics.org/knb/d1/mn/v2/packages/application%2Fbagit-1.0/urn%3Auuid%3A06378560-a84f-4bbc-8079-97fa5e8ec947",
dataset_doi = packageId,
license = "CC0 1.0") %>%
  select(all_of(schema))

arrow::write_parquet(merged, here::here("data", "cleaned", "pink_mountain.parquet"))
