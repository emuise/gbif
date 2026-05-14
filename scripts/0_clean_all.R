library(magrittr)
library(purrr)
files <- fs::dir_ls(here::here("scripts", "cleaning")) 

# salamander is the first one i did; it sets up the schema
ordered <- c(files[grep("salamander", files)], files[!grepl("salamander", files)]) %>%
  names()

bases <- ordered %>%
  basename() %>%
  tools::file_path_sans_ext()

missing_ind <- here::here("data", "cleaned", glue::glue("{bases}.parquet")) %>%
  fs::file_exists()

ordered[!missing_ind] %>%
  str_subset("edna", negate = T) %>% # not done yet
  str_subset("global_population_dynamics", negate = T) # not viable due to spatial issues

ordered[!missing_ind] %>%
  str_subset("edna", negate = T) %>% # not done yet
  str_subset("global_population_dynamics", negate = T) %>% # not viable due to spatial issues
  map(source, local = T)

