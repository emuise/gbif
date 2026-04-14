library(tidyverse)
library(terra)
library(googledrive)

drive_auth(scopes = "https://www.googleapis.com/auth/drive")

edna_dir <- here::here("data", "edna")
fs::dir_create(edna_dir)



# 1. Get the HTML of the public folder page
folder_url <- "https://drive.google.com/drive/folders/1gvoMGYMvJ7prSSFXYhSVQgfQ-meBDN7O"
page <- rvest::read_html(folder_url)

# 2. Extract the JSON-like data Google embeds in the page
# This is a bit 'hacky' but works for public folders when the API blocks you
script_data <- page %>% 
  rvest::html_nodes("script") %>% 
  rvest::html_text()

# annoying way to extract the infrmation i am looking for
# urls are in the 35th index, splitting apart based on what
# appears to be the header and footer, and removing annoying values
# then split on "," and unlist to have a searchable character vector

split_data <- script_data[[36]] %>%
  str_split(';') %>%
  .[[1]] %>%
  str_split(' = ') %>%
  .[[1]] %>%
  .[[2]] %>%
  str_remove_all("'") %>%
  str_split(",") %>%
  unlist() 

urls <- split_data %>%
  str_subset("https") %>%
  str_remove_all("x22") %>%
  str_remove_all("\\\\")

map(urls, \(x) {
  md <- as_id(x) %>%
    drive_get()

  savename <- here::here(edna_dir, md$name)

  type = NULL
  
  if(tools::file_ext(md$name) == "") {
    type = "docx"
    savename <- glue::glue(tools::file_path_sans_ext(savename), ".", type)
  }

  if(file.exists(savename)) {
    return(savename)
  }

  drive_download(md$id, path = here::here(edna_dir, md$name), type = type)
})
