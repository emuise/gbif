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
dataverse_df <- get_dataset("10.5683/SP2/BPLPAP")$files %>%
  as_tibble(.name_repair = "minimal") %>%
  select(label, id)

data_folder <- here::here("data", "dataverse", "birds_melles")
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
census <- here::here(data_folder, "Bird_Data_Melles.1994.csv") %>%
  read_csv() %>%
  janitor::clean_names() %>%
  fill(date, .direction = "down") %>%
  select(
    point_number,
    date,
    time_start,
    time_end,
    species,
    abundance_97,
    abundance_98
  ) %>%
  mutate(abundance_97 = as.numeric(na_if(abundance_97, "."))) %>% # i don't know if these should be 0s or NA, default to NA
  pivot_longer(
    starts_with("abundance"),
    names_to = "year",
    values_to = "abundance",
    names_prefix = "abundance_"
  ) %>%
  mutate(year = as.numeric(year) + 1900) %>%
  mutate(date = make_date(year, month(date), day(date))) %>%
  select(-year) %>%
  filter(point_number != "edge pt") %>%
  mutate(
    site_letter = str_sub(
      point_number,
      nchar(point_number),
      nchar(point_number)
    ),
    site_number = str_sub(point_number, 1, nchar(point_number) - 1)
  ) %>%
  # fixing weird bird codes - WIWA 7 notes isn't clear
  # there is no GCKI references in the thesis or the notes, most likely it is RCKI
  # RSTO is likely the Rufous-sided towhee https://en.wikipedia.org/wiki/Rufous-sided_towhee
# https://a100.gov.bc.ca/pub/eirs/finishDownloadDocument.do;jsessionid=0B8D4BFD7DB343CD78B2C45C5112E49F?subdocumentId=8285
# which is no longer a species. it was split into the spotted towhee and the eastern towhee
# this is BC, with this assumption, we can say it is probbably the spotted towhee (i love this bird)
# i am going to manually add it
  mutate(
    species = replace_when(species, 
      species == "WIWA (7 NOTES)" ~ "WIWA",
      species == "RCKI OR GCKI?" ~ "RCKI",
      species == "RSTO" ~ "SPTO"
    )
  )

# kml files are always in WGS84, no conversion needed
sites <- here::here(data_folder, "Melles.1994_Corrected_Sites.kml") %>%
  vect() %>%
  filter(str_detect(Name, "omit", negate = T)) %>%
  mutate(
    site_letter = str_sub(
      Name,
      1,
      1
    ),
    site_number = str_sub(Name, 2, nchar(Name))
  ) %>%
  filter(site_number != "44a")



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
