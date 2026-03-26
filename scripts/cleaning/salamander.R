library(tidyverse)
library(terra)
library(tidyterra)
library(bcmaps)
library(dataverse)
library(pdftools)

oldhere <- here::here()

# you will need to set this to your dataverse API access key
# Sys.setenv("DATAVERSE_KEY" = "yourkeyhere")
# and set the dataverse server to be UBC
Sys.setenv("DATAVERSE_SERVER" = "borealisdata.ca")

# custom setup for each file
sal_dataverse_df <- get_dataset("10.5683/SP3/7MH2AM")$files %>%
  as_tibble(.name_repair = "minimal") %>%
  select(label, id)

sal_data_id <- sal_dataverse_df %>%
  dplyr::filter(label == "data_and_data_information.zip") %>%
  pull(id)

sal_bin <- get_file_by_id(sal_data_id)

sal_zip_loc <- here::here("data", "dataverse", "salamander_data.zip")
sal_uz_loc <- tools::file_path_sans_ext(sal_zip_loc)
fs::dir_create(dirname(sal_zip_loc))

writeBin(sal_bin, sal_zip_loc)

unzip(sal_zip_loc, exdir = sal_uz_loc)

# original script as downloaded has an error: the Pro3 script is accidentally renamed to Nes19
# these lines fix that using manual line edits on the .R file, and then sourcing the updated
# script. I defensively wrapped this as it uses setwd() and don't want to deal with any
# unintended effects from that, even if i setwd() back
script_loc <- here::here(
  sal_uz_loc,
  "data_and_data_information",
  "CGS_data_files_manipulations.R"
)
newscript_loc <- str_replace(script_loc, ".R", "_updated.R")

if (!file.exists(newscript_loc)) {
  sal_script <- read_lines(script_loc)

  # switch the setwd() programmatically
  sal_script[[10]] <- glue::glue("setwd('{dirname(script_loc)}')")

  # fix the misnamed site
  sal_script[[409]] <- str_replace(sal_script[[409]], "Nes19", "Pro3")

  # rename save location
  sal_script[[432]] <- str_replace(
    sal_script[[432]],
    "long.csv",
    "long_updated.csv"
  )

  # rename save location
  sal_script[[537]] <- str_replace(
    sal_script[[537]],
    "long.csv",
    "long_updated.csv"
  )

  # rename save location
  sal_script[[611]] <- str_replace(
    sal_script[[611]],
    "long.csv",
    "long_updated.csv"
  )

  # the folq site does not have the date properly dealt with
  # metadata says folq and tamc should have the same asterisk removed
  # copying the code (line 142) from tamc section
  # into the folq section
  # we are using append, so we do this LAST so as to not
  # mess up earlier line number based adjustements
  sal_script <- append(sal_script, sal_script[[142]], after = 344)

  # write new R file
  write_lines(
    sal_script,
    here::here(
      sal_uz_loc,
      "data_and_data_information",
      "CGS_data_files_manipulations_updated.R"
    )
  )

  # i dont want to keep all the intermediate data from the script
  # so i am using local = new.env()
  suppressMessages(
    source(newscript_loc, local = new.env(), echo = F)
  )

  
}

setwd(oldhere)

# read the newly updated, clean salamander data
sal_data <- here::here(
  sal_uz_loc,
  "data_and_data_information",
  "CGS_modified_combined_data_1994_2001_long_updated.csv"
) %>%
  arrow::read_csv_arrow()

# get and download the pdf file from the dataverse
sal_pdf_id <- sal_dataverse_df %>%
  dplyr::filter(label == "README.pdf") %>%
  pull(id)

sal_pdf_loc <- here::here(sal_uz_loc, "readme.pdf")

writeBin(get_file_by_id(831605), sal_pdf_loc)


pdf_data <- pdftools::pdf_data(sal_pdf_loc)

# second page, this is where the table is
p2 <- pdf_data[[2]]

# extract northings and eastings from the table
# this code is only designed to be used with this dataset
# and only because we are using a specific version do I do it
# in this way
# i have manually double and triple checked that this works out to the
# streams being correctly matched with their northings and eastings
eastings <- p2 %>%
  filter(endsWith(text, "E,")) %>%
  pull(text)

northings <- p2 %>%
  filter(endsWith(text, "N")) %>%
  pull(text)

streams <- sal_data %>%
  pull(Stream) %>%
  unique()

# this extracts any matches of the stream names from the salamander data
# again, something that can only be done because the data is versioned
streams_pdf <- p2 %>%
  filter(str_detect(text, paste(streams, collapse = "|"))) %>%
  pull(text)

# this is now a jointable
stream_locs <- tibble(
  Stream = streams_pdf,
  easting = eastings,
  northing = northings
) %>%
  mutate(Stream = replace_values(Stream, "Nes19d" ~ "Nes19")) %>%
  mutate(easting = str_remove(easting, ","))

# chilliwack is in UTM10N i think?
# spatial information on streams, needs to be projected to match others

library(terra)
library(tidyterra)

plot(
  bc_bound() %>%
    vect() %>%
    project("EPSG:32610")
)

stream_v <- stream_locs %>%
  mutate(
    easting_num = as.numeric(str_remove(easting, "E")),
    northing_num = as.numeric(str_remove(northing, "N"))
  ) %>%
  vect(geom = c("easting_num", "northing_num"), crs = "EPSG:32610") %>%
  select(-easting, -northing)

plot(stream_v, add = T)

# the salamander data needs to have the species information and other metadata included
sal_v <- left_join(
  sal_data,
  stream_v %>%
    st_as_sf()
) %>%
  st_as_sf(crs = "EPSG:32610") %>%
  vect() %>%
  janitor::clean_names()

# this is a cleaner version of the dataset that is also spatial

# species is coastal giant salamander (*Dicamptodon tenebrosus*)
library(taxize)
library(lubridate)

valid_scientific_name <- "Dicamptodon tenebrosus"

dt_class <- classification(valid_scientific_name, db = "gbif")

spec_info <- dt_class[[1]] %>%
  as_tibble() %>%
  select(-id) %>%
  pivot_wider(names_from = rank, values_from = name)


sal_wgs <- sal_v %>%
  project("EPSG:4326")

wgs_crd <- crds(sal_wgs) %>%
  as_tibble() %>%
  rename(longitude = x, latitude = y)

sal_clean <- sal_wgs %>%
  as_tibble() %>%
  bind_cols(wgs_crd) %>%
  select(date, longitude, latitude, location_m) %>%
  mutate(
    # change various mislabelled values into their numeric counterparts
    location_m = location_m %>%
      # 1. Replace multiple patterns at once
      replace_values(
        c(".", "O") ~ "0",
        "minus 5" ~ "-5",
      ) %>%
      # 2. Convert to numeric
      as.numeric() %>%
      # 3. Replace NAs with the maximum value found in the vector
      {
        if_else(is.na(.), max(., na.rm = TRUE), .)
      }
  ) %>%
  mutate(
    year_obs = year(date),
    month_obs = month(date),
    day_obs = day(date),
    time_obs = NA,
    coordinate_uncertainty = as.numeric(location_m) + 10, # this is assuming an additional 10 m of gps accuracy
    coordinate_uncertainty_unit = "meter"
  ) %>%
  select(-date, -location_m)


# clean it up to match the quebecois/e
metadata <- tibble(
  valid_scientific_name = valid_scientific_name,
  observation_value = 1,
  observation_type = "occurrence",
  effort_sampling_value = NA,
  effort_sampling_method = NA,
  effort_sampling_unit = NA,
  spec_info %>% 
    select(-species), # listed as valid_scientific_name
  observed_rank = "species",
  group = "Amphibians",
  vernacular = "Coastal Giant Salamander",
  license = "CC-BY-4.0",
  dataset_doi = "10.5683/SP3/7MH2AM",
  dataset_url_information = "https://borealisdata.ca/dataset.xhtml?persistentId=doi:10.5683/SP3/7MH2AM",
  dataset_url_download = NA,
  dataset_publisher = "Borealis",
  dataset_creator = "John Richardson",
  dataset_name = "Population Survey Data for Coastal Giant Salamanders in the Chilliwack River Valley"
)

# names and ordering from biodiversite quebec
source("http://atlas.biodiversite-quebec.ca/bq-atlas-parquet.R")

qc_names <- colnames(atlas_remote(tail(atlas_dates$dates, n = 1)))

merged <- bind_cols(sal_clean, metadata) %>%
  relocate(any_of(qc_names)) %>%
  relocate(vernacular, group, .before = observed_rank)

save_loc <- here::here("data", "living_data_cleaned", "salamanders_BQ_format.parquet")
fs::dir_create(dirname(save_loc))

arrow::write_parquet(merged, save_loc)
