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

data_folder <- here::here("data", "raw_data", "dataverse", "birds_weber")
fs::dir_create(data_folder)

# get all of the data

dataverse_df %>%
  pmap(
    \(label, id) {
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
    },
    .progress = T
  )

# species first to fix dunbar from symbol to species
# sharp shinned hawk typo
# norther harrier typo
species <- here::here(data_folder, "Species_Recorded_Weber.1967.csv") %>%
  read_csv() %>%
  janitor::clean_names() %>%
  select(symbol:scientific_name) %>%
  mutate(
    common_name = replace_values(
      common_name,
      "Sharo-shinned Hawk" ~ "Sharp-shinned Hawk",
      "Northern Herrier" ~ "Northern Harrier"
    )
  )

# the census is missing the 22nd and dunbar
# and 33nd and dunbar tables from page 221 of the thesis (page 248 in pdf)
# i am going to transcribe them to a tribble and then reformat them to match
# i am encoding ".." as NA as its likely an absence and not technically a 0
# i cant think of a better way of doing this programmatically
dunbar22 <- tribble(
  ~symbol , ~`1969-12-05` , ~`1969-12-07` , ~`1969-12-12` , ~`1970-01-06` , ~`1970-01-10` , ~`1970-02-01` , ~`1970-02-21` , ~`1970-03-04` ,
  "OJ"    ,            20 ,            17 ,            20 ,            23 ,            23 ,            22 ,            22 ,            18 ,
  "HS"    ,            18 ,            20 ,            14 ,            10 ,            17 ,            31 ,            32 ,            10 ,
  "HF"    ,             2 , NA            ,             6 ,             9 ,             3 ,             8 ,             9 ,             3 ,
  "BCC"   ,             4 ,             7 ,             3 ,             2 ,             7 ,             4 ,             5 ,             8 ,
  "S"     ,             3 ,            16 ,            50 , NA            ,            13 ,            14 ,            25 , NA            ,
  "AR"    , NA            , NA            , NA            ,             1 ,             5 , NA            ,            17 ,             5 ,
  "SS"    ,             8 ,             6 ,             7 ,             8 ,             6 ,             5 ,             6 ,             4 ,
  "ST"    ,             2 ,             1 ,             5 ,             4 ,             9 ,             9 ,             4 ,             6 ,
  "PSK"   , NA            , NA            , NA            , NA            , NA            , NA            ,             6 , NA            ,
  "CP"    , NA            ,            10 ,             8 , NA            ,             7 ,             1 ,             3 ,            10 ,
  "PF"    , NA            ,             9 , NA            ,             4 ,             3 , NA            , NA            , NA            ,
  "BT"    , NA            , NA            , NA            , NA            , NA            , NA            , NA            ,             1 ,
  "RC"    , NA            , NA            , NA            , NA            , NA            , NA            , NA            , NA            ,
  "SJ"    , NA            , NA            , NA            , NA            , NA            , NA            , NA            , NA            ,
  "CBC"   , NA            , NA            , NA            , NA            , NA            , NA            , NA            , NA            ,
  "RSF"   ,             1 , NA            , NA            ,             1 ,             2 ,             1 ,             1 , NA            ,
  "GWG"   , NA            ,             1 ,             1 , NA            ,             1 ,             3 , NA            ,             1 ,
  "CRP"   , NA            , NA            , NA            , NA            , NA            , NA            , NA            , NA            ,
  "WCS"   , NA            , NA            , NA            , NA            ,             1 ,             3 , NA            , NA            ,
  "DW"    , NA            , NA            , NA            , NA            ,             1 ,             1 , NA            , NA            ,
  "CM"    , NA            , NA            , NA            , NA            , NA            , NA            ,             3 , NA            ,
  "EG"    , NA            , NA            , NA            , NA            ,             1 , NA            , NA            , NA            ,
  "RBN"   , NA            , NA            , NA            , NA            , NA            , NA            , NA            , NA            ,
  "GCK"   , NA            , NA            , NA            , NA            , NA            , NA            , NA            , NA            ,
  "RCK"   , NA            , NA            ,             1 , NA            , NA            , NA            , NA            , NA
) %>%
  pivot_longer(-symbol, names_to = "date", "values_to" = "count") %>%
  mutate(location = NA, plot = "22nd_Dunbar", site = "Vancouver")

dunbar33 <- tribble(
  ~symbol , ~`1969-12-07` , ~`1969-12-09` , ~`1969-12-15` , ~`1970-01-08` , ~`1970-01-26` , ~`1970-02-15` , ~`1970-02-28` , ~`1970-03-09` ,
  "OJ"     ,            30 ,            50 ,            40 ,            17 ,            40 ,            70 ,            70 ,            28 ,
  "HS"     ,            32 ,            34 ,            28 ,            24 ,            20 ,            36 ,            34 ,            22 ,
  "HF"     ,            17 ,            20 ,            28 ,            60 ,            23 ,            36 ,            16 ,            16 ,
  "BCC"    ,            20 ,            20 ,            20 ,            14 ,            14 ,            20 ,            17 ,            17 ,
  "S"      , NA            , NA            ,             3 , NA            ,             1 ,             9 ,             8 ,             3 ,
  "AR"     ,             2 , NA            ,            10 ,             1 ,             1 ,            25 ,            21 ,            16 ,
  "SS"     ,            10 ,             9 ,             5 ,             4 ,             3 ,             6 ,             6 ,             6 ,
  "ST"     ,             2 ,             2 ,             2 , NA            , NA            , NA            , NA            , NA            ,
  "PSK"    , NA            ,            15 , NA            , NA            , NA            , NA            , NA            ,            25 ,
  "CP"     , NA            , NA            , NA            , NA            , NA            , NA            , NA            ,             2 ,
  "PF"     , NA            ,             1 , NA            , NA            , NA            ,             1 ,             8 ,             2 ,
  "BT"     , NA            , NA            ,            20 , NA            , NA            , NA            , NA            , NA            ,
  "RC"     , NA            , NA            , NA            , NA            , NA            , NA            , NA            ,            15 ,
  "SJ"     ,             4 ,             5 , NA            , NA            , NA            ,             1 ,             2 ,             1 ,
  "CBC"    ,             4 ,             2 ,             2 , NA            ,             3 ,             2 , NA            , NA            ,
  "RSF"    , NA            ,             1 ,             1 ,             2 ,             1 , NA            , NA            , NA            ,
  "GWG"    ,             2 , NA            , NA            , NA            , NA            , NA            , NA            , NA            ,
  "CRP"    , NA            , NA            , NA            ,             5 , NA            , NA            , NA            , NA            ,
  "WCS"    , NA            , NA            , NA            , NA            , NA            , NA            , NA            , NA            ,
  "DW"     , NA            , NA            ,             1 , NA            , NA            , NA            , NA            , NA            ,
  "CM"     , NA            , NA            , NA            , NA            , NA            , NA            , NA            , NA            ,
  "EG"     , NA            ,             2 , NA            , NA            , NA            , NA            , NA            , NA            ,
  "RBN"    , NA            , NA            ,             1 ,             1 , NA            , NA            , NA            , NA            ,
  "GCK"    , NA            , NA            , NA            , NA            , NA            ,             2 , NA            , NA            ,
  "RCK"    , NA            , NA            , NA            , NA            , NA            , NA            , NA            , NA
) %>%
  pivot_longer(-symbol, names_to = "date", "values_to" = "count") %>%
  mutate(location = NA, plot = "33rd_Dunbar", site = "Vancouver")

dunbar <- bind_rows(dunbar22, dunbar33) %>%
  mutate(species = replace_values(symbol, from = species$symbol, to = species$common_name),
  date = ymd(date)) %>%
  select(-symbol) %>%
  filter(!is.na(count)) %>% # removes 0s as they arent found there
  relocate(species) 

census <- here::here(data_folder, "Bird_Census_Total_Weber.1967.csv") %>%
  read_csv() %>%
  janitor::clean_names() %>%
  mutate(date = mdy(date)) %>%
  bind_rows(dunbar) %>%
  filter(count != 0,
         site == "Vancouver") %>%
  # typo in name of chickadee
  # western flycatcher got split into two species in 1989
  # https://en.wikipedia.org/wiki/Western_flycatcher
  # we are west of the rockies therefore we are Pacific-slope flycatcher
  # marsh hawk is northern harrier
  mutate(
    species = replace_values(
      species,
      "Black-capped-Chickadee" ~ "Black-capped Chickadee",
      "Western Flycatcher" ~ "Pacific-slope Flycatcher",
      "Marsh Hawk" ~ "Northern Harrier"
    )
  )

left_join(census, species, by = c("species" = "common_name")) %>%
  filter(is.na(scientific_name))


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
  mutate(
    plot = recode_values(
      Name,
      "weber1" ~ "19th_Yukon",
      "weber2" ~ "14th_Spruce",
      "weber3" ~ "43rd_Churchill",
      "weber4" ~ "Ferguson_Road",
      "weber5" ~ "22nd_Dunbar",
      "weber6" ~ "33rd_Dunbar"
    )
  ) %>%
  select(plot)


# GULL is likely gulls, which are "notoriously hard to identify"
# https://www.birdscanada.org/wp-content/uploads/2020/04/Adult_Gulls.pdf
# i cross referenced this list with wikipedia, found that the majority are in genus Larus
# the only one that isn't is Bonaparte's gull. I personally think I (a novice birder)
# could differentiate them, and have confirmed with my bird expert (Liam Irwin) that
# he thinks they are '/easy/' to id, but also have significantly different
# behaviour from normal gulls. he confirmed that an msc level birder should be able to pick
# them out. due to this i am labelling GULL as Larus species

species_names <- species %>%
  pull(scientific_name)

# rows = 1 again, specifically to pull GULL larus spp correctly
# once again i double checked that rows = 1 is the correct course of action (it is)
species_taxa <- classification(species_names, db = "gbif", rows = 1)

taxa_wide <- species_taxa %>%
  rbind() %>%
  select(-id) %>%
  pivot_wider(names_from = rank, values_from = name)

species_join <- species %>%
  left_join(taxa_wide, by = c("scientific_name" = "query"))

census_taxa <- census %>%
  rename(common_name = species) %>%
  left_join(species_join)

# some sites are missing due to being inaccurate, see details in downloaded data description file
site_census_taxa <- sites_mapped %>%
  left_join(census_taxa) %>%
  select(
    date,
    vernacular = common_name,
    abundance = count,
    kingdom:species
  ) %>%
  filter(abundance != 0) %>%
  # calculate radius as an estimate of geolocation accuracy
  mutate(radii = sqrt(expanse(.) / 3.14159))

ctrd <- centroids(site_census_taxa)

wgs_crd <- crds(ctrd) %>%
  as_tibble() %>%
  rename(longitude = x, latitude = y)

schema <- arrow::read_parquet(
  here::here("data", "cleaned", "salamanders_BQ_format.parquet")
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
    coordinate_uncertainty = radii + 10, # this is assuming  10 m of inaccuracy for GPS
    coordinate_uncertainty_unit = "meter",
    year_obs = year(date),
    month_obs = month(date),
    day_obs = day(date),
    time_obs = NA,
    group = "Birds",
    observed_rank = ifelse(is.na(valid_scientific_name), "genus", "species"),
    dataset_name = "Data for: Birds in cities: A study of populations, foraging ecology and nest-sites of urban birds.",
    dataset_creator = "Weber, Wayne C.",
    dataset_publisher = "Borealis",
    dataset_url_information = "https://borealisdata.ca/dataset.xhtml?persistentId=doi:10.5683/SP2/K5LMLA",
    dataset_url_download = NA,
    dataset_doi = "10.5683/SP2/K5LMLA",
    license = "CC0 1.0"
  ) %>%
  select(all_of(schema))

save_loc <- here::here(
  "data",
  "cleaned",
  "weber_birds_1972.parquet"
)
fs::dir_create(dirname(save_loc))

arrow::write_parquet(merged, save_loc)
