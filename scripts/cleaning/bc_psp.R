library(tidyverse)
library(terra)
library(tidyterra)
library(taxize)
library(rvest)

options(timeout = 3000)

data_loc <- here::here("data", "raw_data", "bc_psp")

psp_loc <- "https://www.for.gov.bc.ca/ftp/HTS/external/!publish/ground_plot_compilations/psp/"

non_psp_loc <- "https://www.for.gov.bc.ca/ftp/HTS/external/!publish/ground_plot_compilations/non_psp/publish_province/"


download_folder <- function(url) {
  download_file <- function(url2) {
    file <- basename(url2)

    savename <- here::here(save_fold, file)

    if (fs::file_exists(savename)) {
      return(savename)
    }

    download.file(url2, savename)
  }

  # stupid regex to just get the base url because i didnt want to use another package
  base_url <- strsplit(url, "(?<!/)/(?!/)", perl = TRUE)[[1]][1]

  subfold <- basename(url)

  save_fold <- here::here(data_loc, subfold)
  fs::dir_create(save_fold, showWarnings = F)

  links <- read_html(url) %>%
    html_elements("pre a") %>%
    html_attr("href") %>%
    tail(-1) %>% # first object is the go back a page
    paste0(base_url, .)

  map(links, download_file)
}

download_folder(psp_loc)
download_folder(non_psp_loc)


# CMI, YSM, and NFI plots are generalized locations, so are therefore not useable
# https://catalogue.data.gov.bc.ca/dataset/824e684b-4114-4a05-a490-aa56332b57f4

# data table because its a big one!

### PSP first, all are valid. it is the non-psp that needs to filter out cmi, ysm and nfi.
psp_fold <- here::here(data_loc, "psp")

tree_details <- here::here(psp_fold, "faib_tree_detail.csv") %>%
  data.table::fread() %>%
  janitor::clean_names()

site_locs <- here::here(psp_fold, "faib_header.csv") %>%
  data.table::fread() %>%
  janitor::clean_names()

visit_info <- here::here(psp_fold, "faib_sample_byvisit.csv") %>%
  data.table::fread() %>%
  janitor::clean_names()


trees <- tree_details %>%
  select(site_identifier, clstr_id, visit_number, plot, species) %>%
  filter(
    species != "", # empty string, not ided?
    species != "XC", # unknown softwood
    species != "Z", # unknown
    species != "XH"  # unknown hardwood
  )

sites <- site_locs %>%
  select(site_identifier, longitude, latitude)

visits <- visit_info %>%
  select(site_identifier, clstr_id, visit_number, meas_dt)

# species

psp_species <- trees$species %>% 
  unique() %>% 
  str_subset("MR", negate = T) # waiting for piotr to fix

species_join <- tibble(psp_species) %>%
  mutate(
    scientific_name = CanadaForestAllometry::translate_species_code(
      psp_species,
      from = "jurisdiction",
      jurisdiction = "bc",
      to = "scientificname"
    ),
    common_name = CanadaForestAllometry::translate_species_code(
      psp_species,
      from = "jurisdiction",
      jurisdiction = "bc",
      to = "englishname"
    ) %>%
      str_to_title()
  )

toj <- taxize::classification(species_join$scientific_name, db = "gbif", rows = 1) %>%
  rbind() %>%
  select(-id) %>%
  pivot_wider(names_from = rank, values_from = name)

species_join <- species_join %>% left_join(toj, by = c("scientific_name" = "query"))

trees_taxa <- trees %>%
  left_join(species_join, by = c("species" = "psp_species")) %>%
  select(-species) %>%
  rename(species = species.y)

schema <- arrow::read_parquet(
  here::here("data", "cleaned", "salamander.parquet")
) %>%
  names()

all <- trees_taxa %>% 
  left_join(sites) %>%
  left_join(visits)

psp_merged <- all %>%
  filter_out(is.na(longitude)) %>%
  filter_out(is.na(latitude)) %>%
  filter_out(is.na(meas_dt)) %>%
  mutate(valid_scientific_name = scientific_name,
  observation_value = 1,
observation_type = "occurrence",
effort_sampling_value = NA,
effort_sampling_unit = NA,
effort_sampling_method = NA,
coordinate_uncertainty = 100, # some of these are based on geolocating a map
coordinate_uncertainty_unit = "meter",
year_obs = year(meas_dt),
month_obs = month(meas_dt),
day_obs = day(meas_dt),
time_obs = NA,
vernacular = common_name,
group = "Trees",
observed_rank = ifelse(is.na(species), "genus", "species"),
dataset_name = "Forest Inventory Ground Plot Data",
dataset_creator = "Forest Analysis and Inventory Branch",
dataset_publisher = "BC Data Catalogue",
dataset_url_information = "https://catalogue.data.gov.bc.ca/dataset/824e684b-4114-4a05-a490-aa56332b57f4",
dataset_url_download = "https://www.for.gov.bc.ca/ftp/HTS/external/!publish/ground_plot_compilations/",
dataset_doi = NA,
license = "OGL - British Columbia"
) %>%
  select(all_of(schema))

### non-psp that needs to filter out cmi, ysm and nfi.
npsp_fold <- here::here(data_loc, "publish_province")

tree_details <- here::here(npsp_fold, "faib_tree_detail.csv") %>%
  data.table::fread() %>%
  janitor::clean_names()

site_locs <- here::here(npsp_fold, "faib_header.csv") %>%
  data.table::fread() %>%
  janitor::clean_names()

visit_info <- here::here(npsp_fold, "faib_sample_byvisit.csv") %>%
  data.table::fread() %>%
  janitor::clean_names()


trees <- tree_details %>%
  select(site_identifier, clstr_id, visit_number, plot, species) %>%
  filter(
    species != "", # empty string, not ided?
    species != "XC", # unknown softwood
    species != "Z", # unknown
    species != "XH"  # unknown hardwood
  )

sites <- site_locs %>%
  filter_out(sample_establishment_type %in% c("CMI", "YSM", "NFI")) %>%
  select(site_identifier, longitude, latitude)

visits <- visit_info %>%
  select(site_identifier, clstr_id, visit_number, meas_dt)

# species

npsp_species <- trees$species %>% 
  unique() %>% 
  str_subset("MR", negate = T) # waiting for piotr to fix

species_join <- tibble(npsp_species) %>%
  mutate(
    scientific_name = CanadaForestAllometry::translate_species_code(
      npsp_species,
      from = "jurisdiction",
      jurisdiction = "bc",
      to = "scientificname"
    ),
    common_name = CanadaForestAllometry::translate_species_code(
      npsp_species,
      from = "jurisdiction",
      jurisdiction = "bc",
      to = "englishname"
    ) %>%
      str_to_title()
  )

toj <- taxize::classification(species_join$scientific_name, db = "gbif", rows = 1) %>%
  rbind() %>%
  select(-id) %>%
  pivot_wider(names_from = rank, values_from = name)

species_join <- species_join %>% left_join(toj, by = c("scientific_name" = "query"))

trees_taxa <- trees %>%
  left_join(species_join, by = c("species" = "npsp_species")) %>%
  select(-species) %>%
  rename(species = species.y)

schema <- arrow::read_parquet(
  here::here("data", "cleaned", "salamander.parquet")
) %>%
  names()

all <- trees_taxa %>% 
  left_join(sites) %>%
  left_join(visits)

npsp_merged <- all %>%
  filter_out(is.na(longitude)) %>%
  filter_out(is.na(latitude)) %>%
  filter_out(is.na(meas_dt)) %>%
  mutate(valid_scientific_name = scientific_name,
  observation_value = 1,
observation_type = "occurrence",
effort_sampling_value = NA,
effort_sampling_unit = NA,
effort_sampling_method = NA,
coordinate_uncertainty = 100, # some of these are based on geolocating a map
coordinate_uncertainty_unit = "meter",
year_obs = year(meas_dt),
month_obs = month(meas_dt),
day_obs = day(meas_dt),
time_obs = NA,
vernacular = common_name,
group = "Trees",
observed_rank = ifelse(is.na(species), "genus", "species"),
dataset_name = "Forest Inventory Ground Plot Data",
dataset_creator = "Forest Analysis and Inventory Branch",
dataset_publisher = "BC Data Catalogue",
dataset_url_information = "https://catalogue.data.gov.bc.ca/dataset/824e684b-4114-4a05-a490-aa56332b57f4",
dataset_url_download = "https://www.for.gov.bc.ca/ftp/HTS/external/!publish/ground_plot_compilations/",
dataset_doi = NA,
license = "OGL - British Columbia"
) %>%
  select(all_of(schema))

merged <- bind_rows(psp_merged, npsp_merged)


arrow::write_parquet(merged, here::here("data", "cleaned", "bc_psp.parquet"))
