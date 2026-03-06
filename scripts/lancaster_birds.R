library(tidyverse)
library(terra)
library(tidyterra)
library(bcmaps)
library(dataverse)
library(taxize)

oldhere <- here::here()

# you will need to set this to your dataverse API access key
# Sys.setenv("DATAVERSE_KEY" = "yourkeyhere")
# and set the dataverse server to be UBC
Sys.setenv("DATAVERSE_SERVER" = "borealisdata.ca")

# custom setup for each file
dataverse_df <- get_dataset("10.5683/SP2/YD6N7C")$files %>%
  as_tibble(.name_repair = "minimal") %>%
  select(label, id)

data_folder <- here::here("data", "dataverse", "birds_lancaster")
fs::dir_create(data_folder)

# get all of the data

dataverse_df %>%
  pmap(\(label, id) {
    savename <- here::here(data_folder, label)

    if (tools::file_ext(label) == "tab") {
      form = "bundle"
      savename <- savename %>%
        tools::file_path_sans_ext() %>%
        glue::glue(".zip")
    } else {
      form = "original"
    }

    bin <- get_file_by_id(id, format = form)

    if (file.exists(savename)) {
      return(savename)
    }

    writeBin(bin, savename)
    if (tools::file_ext(savename) != "zip") {
      return(savename)
    }
    csv <- unzip(savename, list = T) %>%
      pull(Name) %>%
      str_subset(".csv")
    unzip(savename, files = csv, exdir = data_folder)

    file.remove(savename)

    return(here::here(data_folder, csv))
  })

#  density is in #/40ha, so we will need to convert to #/ha
census_long <- here::here(data_folder, "Bird_Census_Lancaster.1974.csv") %>%
  read_csv() %>%
  janitor::clean_names() %>%
  filter(species != "Total") %>%
  pivot_longer(cols = dt:ub, names_to = "site", values_to = "density") %>%
  mutate(density = density / 40) # convert to #/ha

# kml files are always in WGS84, no conversion needed
sites <- here::here(data_folder, "Lancaster.1974_sites.kml") %>%
  vect() %>%
  mutate(
    name = Name %>%
      str_remove("Lancaster_") %>%
      str_to_lower()
  ) %>%
  select(site = name)

species <- here::here(data_folder, "Species_Recorded_Lancaster.1974.csv") %>%
  read_csv() %>%
  janitor::clean_names()

species_names <- species %>%
  pull(scientific_name)

# i have confirmed that the first row for all is correct when multiple options are available
species_taxa <- classification(species_names, db = "gbif", rows = 1)

taxa_wide <- species_taxa %>%
  rbind() %>%
  select(-id) %>%
  pivot_wider(names_from = rank, values_from = name)


species_join <- species %>% 
  left_join(taxa_wide, by = c("scientific_name" = "query"))  %>%
  select(-no)

census_taxa <- census_long %>%
  rename(code = species) %>%
  left_join(species_join)

site_census_taxa <- sites %>%
  left_join(census_taxa)
