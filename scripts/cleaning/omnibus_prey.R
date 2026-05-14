library(tidyverse)
library(dataone)
# windows workaround for an error in dataone as of april 14, 2024
# write('CURL_SSL_BACKEND=openssl', file = "~/.Renviron", append = TRUE)
library(terra)
library(tidyterra)
library(taxize)
data_loc <- here::here("data", "raw_data", "dataone", "omnibus_prey")
fs::dir_create(data_loc)

cn <- CNode("PROD")
mn <- getMNode(cn, "urn:node:KNB")


# Define the identifier
packageId <- "doi:10.5063/F15H7DS3"

# Download the package
pkg <- getPackage(mn, packageId)

# nicely named ddata
unzip(pkg, exdir = data_loc)

diets <- here::here(data_loc, "data", "RaptorDiets.csv") %>%
  read_csv() %>%
  janitor::clean_names()

# when you read this metadata, there are three read errors related to commas found in the Year column.
# none of these are within (or near) Canada, so I ignore it
metadata <- here::here(data_loc, "data", "RaptorDiets_metadata.csv") %>%
  read_csv() %>%
  janitor::clean_names()

md_spatial <- vect(
  metadata,
  geom = c("longitude_decimal_degree", "latitude_decimal_degree"),
  crs = "EPSG:4326"
)


bcb <- bcmaps::bc_bound_hres() %>%
  vect()

bcb_wgs <- bcb %>%
  project("EPSG:4326")

# spatial information
md_bc <- intersect(md_spatial, bcb_wgs)

studies <- md_bc %>%
  pull(data_set)

# species information

# i was planning to include direct observations of prey as
# an observation of the prey, but only one study contains
# only direct observations. (Cava et al. 2012; Wilson Journal of Ornithology)
# reading this paper, it seems unlikely that all data is confirmed to be
# direct observations, as the study includes prey remains
# i am going to not include any prey data, just the raptors.
studies_direct_obs <- md_bc %>%
  filter(!is.na(direct_observations)) %>%
  pull(data_set)

diets_bc <- diets %>%
  filter(data_set %in% studies)

species_names <- diets_bc %>%
  pull(raptor_scientific_name) %>%
  unique()
taxa_wide <- taxize::classification(species_names, db = "gbif") %>%
  rbind() %>%
  select(-id) %>%
  pivot_wider(names_from = rank, values_from = name)

species_joined <- diets_bc %>%
  select(data_set, raptor_scientific_name) %>%
  left_join(taxa_wide, by = c("raptor_scientific_name" = "query")) %>%
  distinct()

# temporal information
# the only real valid temporal information is year
# doing based on quarters is very tricky.
md_t <- metadata %>%
  filter(data_set %in% studies) %>%
  select(
    data_set,
    latitude = latitude_decimal_degree,
    longitude = longitude_decimal_degree,
    year
  )


schema <- arrow::read_parquet(
  here::here("data", "cleaned", "salamander.parquet")
) %>%
  names()

# joined
merged <- left_join(species_joined, md_t) %>%
  select(valid_scientific_name = species,
  longitude,
latitude,
year_obs = year,
kingdom, phylum, class, order, family, genus, raptor_scientific_name) %>%
  mutate(observation_value = 1,
  observation_type = "occurence",
effort_sampling_value = NA,
effort_sampling_unit = NA,
effort_sampling_method = NA,
coordinate_uncertainty = 10000,
coordinate_uncertainty_unit = "meter",
month_obs = NA,
day_obs = NA,
time_obs = NA,
vernacular = raptor_scientific_name,
group = "Birds",
observed_rank = "species",
dataset_name = "OS-Prey (Omnibus study of prey) V3",
dataset_creator = "Stella F. Uiterwaal",
dataset_publisher = "Knowledge Network for Biodiversity",
dataset_url_information = "https://knb.ecoinformatics.org/view/doi%3A10.5063%2FF15H7DS3",
dataset_url_download = "https://knb.ecoinformatics.org/knb/d1/mn/v2/packages/application%2Fbagit-1.0/urn%3Auuid%3A1b2ff978-6ce9-4927-b0e6-0891387faef6",
dataset_doi = "doi:10.5063/F15H7DS3",
license = "CC-BY-4.0") %>%
  select(all_of(schema))


arrow::write_parquet(merged, here::here("data", "cleaned", "omnibus_prey.parquet"))
