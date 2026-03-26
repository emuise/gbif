library(tidyverse)
library(terra)
library(tidyterra)
library(bcmaps)
library(dataverse)
library(taxize)

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
      str_subset(".csv|.xlsx")
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
  mutate(density_ha = density / 40) %>% # convert to #/ha
  select(-density) %>%
  # we don't have a matched species for BT (an infrequent observation) in the species list
  # it is likely not a typo of BTP, as it shows up in all the tables
  # searching Lancaster's thesis doesn't reveal anything. due to this
  # omitting from the dataset - we hae no idea what it is other than a bird
  filter(species != "BT")


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
# this will need to be done for each new file
species_taxa <- classification(species_names, db = "gbif", rows = 1)

taxa_wide <- species_taxa %>%
  rbind() %>%
  select(-id) %>%
  pivot_wider(names_from = rank, values_from = name)


species_join <- species %>%
  left_join(taxa_wide, by = c("scientific_name" = "query")) %>%
  select(-no)

census_taxa <- census_long %>%
  rename(code = species) %>%
  left_join(species_join)

site_census_taxa <- sites %>%
  left_join(census_taxa) %>%
  select(season, vernacular = common_name, density_ha, kingdom:species) %>%
  filter(density_ha != 0) %>%
  # calculate radius as an estimate of geolocation accuracy
  mutate(radii = sqrt(expanse(.) / 3.14159))

ctrd <- centroids(site_census_taxa)

wgs_crd <- crds(ctrd) %>%
  as_tibble() %>%
  rename(longitude = x, latitude = y)

schema <- arrow::read_parquet(
  here::here("data", "living_data_cleaned", "salamanders_BQ_format.parquet")
) %>%
  names()


merged <- ctrd %>%
  as_tibble() %>%
  bind_cols(wgs_crd) %>%
  rename(valid_scientific_name = species, observation_value = density_ha) %>%
  mutate(
    observation_type = "density_ha",
    effort_sampling_value = NA,
    effort_sampling_unit = NA,
    effort_sampling_method = NA,
    coordinate_uncertainty = radii + 10, # this is assuming an additional 10 m of inaccuracy due to transforming to a cirlce
    coordinate_uncertainty_unit = "meter",
    year_obs = 1974,
    # dates are based on middle of metadata file: Lancaster.1974_Data_Management_&_Descriptions.csv
    # winter is by far the longest season
    # spring is april 11 to may 31 (may 6 centre)
    # summer is june 1 to august 25 (july 13 centre)
    # autumn is august 26 to october 21 (september 23 centre)
    # winter is october 22 - april 10 (january 15 centre)
    month_obs = case_when(
      season == "Spring" ~ 5,
      season == "Summer" ~ 7,
      season == "Autumn" ~ 9,
      season == "Winter" ~ 1
    ),
    day_obs = case_when(
      season == "Spring" ~ 6,
      season == "Summer" ~ 13,
      season == "Autumn" ~ 23,
      season == "Winter" ~ 15
    ),
    time_obs = NA,
    group = "Aves",
    observed_rank = ifelse(is.na(valid_scientific_name), "genus", "species"),
    dataset_name = "Data for: Bird communities in relation to the structure of urban habitats",
    dataset_creator = "Richard Landcaster",
    dataset_publisher = "Borealis",
    dataset_url_information = "https://borealisdata.ca/dataset.xhtml?persistentId=doi:10.5683/SP2/YD6N7C",
    dataset_url_download = NA,
    dataset_doi = "10.5683/SP2/YD6N7C",
    license = "CC0 1.0"
  ) %>%
  select(all_of(schema))

save_loc <- here::here(
  "data",
  "living_data_cleaned",
  "lancaster_birds_1974.parquet"
)
fs::dir_create(dirname(save_loc))

arrow::write_parquet(merged, save_loc)
