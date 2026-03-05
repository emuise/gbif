library(tidyverse)
library(terra)
library(tidyterra)
library(bcmaps)
library(dataverse)

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

dataverse_df %>%
  pmap(\(label, id) {
    savename <- here::here(data_folder, label)

    if(tools::file_ext(label) == "tab") {
      form = "bundle"
      savename <- savename %>%
      tools::file_path_sans_ext() %>%
      glue::glue(".zip")
    } else {
      form = "original"
    }

    bin <- get_file_by_id(id, format = form)

     
    
    if(file.exists(savename)) {
      return(savename)
    }

    writeBin(bin, savename)
    return(savename)
  })
