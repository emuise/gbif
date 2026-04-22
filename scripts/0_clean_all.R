files <- fs::dir_ls(here::here("scripts", "cleaning")) 

# salamander is the first one i did; it sets up the schema
c(files[grep("salamander", files)], files[!grepl("salamander", files)]) %>%
  names() %>%
  map(source)

