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

    if(fs::file_exists(savename)) {
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

