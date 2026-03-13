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
dataverse_df <- get_dataset("10.5683/SP2/K5LMLA")$files %>%
  as_tibble(.name_repair = "minimal") %>%
  select(label, id)

data_folder <- here::here("data", "dataverse", "birds_weber")
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

# the data appears to be missing many dates
# looking through, based on sites and times we should just be able
# to do a rolling fill forwards
# it seems like very new date is listed for the first site
census <- here::here(data_folder, "Bird_Census_Total_Weber.1967.csv") %>%
  read_csv() %>%
  janitor::clean_names()

# kml files are always in WGS84, no conversion needed
# i need to manually recode the names to match the census data

sites <- here::here(data_folder, "Weber.1967_Sites.kml") %>%
  vect()

# had to use the thesis maps to figure out weber5 and weber6
# weber5 is north of weber6. street names in this dataset
# appear to be wrong? should be 22nd/dunbar and 33rd/dunbar
# see page 11A of thesis (27 of pdf)
# https://open.library.ubc.ca/media/stream/pdf/831/1.0101293/2
# rockcliffe park and alta vista i believe are in ontario
# confirmed in thesis, they do not have a ocrresponding mapped site
# page 23A in thesis, 49 in pdf
sites_mapped <- sites %>%
  mutate(plot = recode_values(Name, "weber1" ~ "19th_Yukon",
"weber2" ~ "14th_Spruce",
"weber3" ~ "43rd_Churchill",
"weber4" ~ "Ferguson_Road",
"weber5" ~ "F_21st",
"weber6" ~ "S_24th"))


species <- here::here(data_folder, "Bird_Names_Melles.1994.csv") %>%
  read_csv() %>%
  janitor::clean_names() %>%
  select(code = x4_letter_code, scientific_name, common_name) 



# GULL is likely gulls, which are "notoriously hard to identify"
# https://www.birdscanada.org/wp-content/uploads/2020/04/Adult_Gulls.pdf
# i cross referenced this list with wikipedia, found that the majority are in genus Larus
# the only one that isn't is Bonaparte's gull. I personally think I (a novice birder)
# could differentiate them, and have confirmed with my bird expert (Liam Irwin) that
# he thinks they are '/easy/' to id, but also have significantly different
# behaviour from normal gulls. he confirmed that an msc level birder should be able to pick
# them out. due to this i am labelling GULL as Larus species

custom_species <- tribble(~code, ~scientific_name, ~common_name,
  "GULL", "Larus spp.", "Gull")

all_species <- bind_rows(species, custom_species)

species_names <- all_species %>%
  pull(scientific_name)

# rows = 1 again, specifically to pull GULL larus spp correctly
species_taxa <- classification(species_names, db = "gbif", rows = 1)

taxa_wide <- species_taxa %>%
  rbind() %>%
  select(-id) %>%
  pivot_wider(names_from = rank, values_from = name)

species_join <- all_species %>%
  left_join(taxa_wide, by = c("scientific_name" = "query"))

census_taxa <- census %>%
  rename(code = species) %>%
  left_join(species_join)

# some sites are missing due to being inaccurate, see details in downloaded data description file
site_census_taxa <- sites %>%
  left_join(census_taxa, by = c("site_letter", "site_number")) %>%
  select(
    date,
    time_start,
    time_end,
    vernacular = common_name,
    abundance,
    kingdom:species
  ) %>%
  filter(abundance != 0)

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
  rename(valid_scientific_name = species, observation_value = abundance) %>%
  mutate(
    observation_type = "abundance",
    effort_sampling_value = NA,
    effort_sampling_unit = NA,
    effort_sampling_method = NA,
    coordinate_uncertainty = 10, # this is assuming  10 m of inaccuracy for GPS
    coordinate_uncertainty_unit = "meter",
    year_obs = year(date),
    # dates are based on middle of metadata file: Lancaster.1974_Data_Management_&_Descriptions.csv
    # winter is by far the longest season
    # spring is april 11 to may 31 (may 6 centre)
    # summer is june 1 to august 25 (july 13 centre)
    # autumn is august 26 to october 21 (september 23 centre)
    # winter is october 22 - april 10 (january 15 centre)
    month_obs = month(date),
    day_obs = day(date),
    time_obs = NA,
    group = "Birds",
    observed_rank = ifelse(is.na(valid_scientific_name), "genus", "species"),
    dataset_name = "Data for: Effects of landscape and local habitat features on bird communities: a study of urban gradient in greater Vancouver.",
    dataset_creator = "Melles, Stephanie J.",
    dataset_publisher = "Borealis",
    dataset_url_information = "https://borealisdata.ca/dataset.xhtml?persistentId=doi:10.5683/SP2/BPLPAP",
    dataset_url_download = NA,
    dataset_doi = "10.5683/SP2/BPLPAP",
    license = "CC0 1.0"
  ) %>%
  select(all_of(schema))

save_loc <- here::here(
  "data",
  "living_data_cleaned",
  "melles_birds_2000.parquet"
)
fs::dir_create(dirname(save_loc))

arrow::write_parquet(merged, save_loc)
