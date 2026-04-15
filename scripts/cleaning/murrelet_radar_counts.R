library(tidyverse)
library(terra)
# figshare needs this package
# pak::pak("mpadge/deposits")
library(deposits)


url <- "https://figshare.com/ndownloader/files/44729566"
data_loc <- here::here("data", "raw", "murrelet")
fs::dir_create(data_loc)

# you need a figshare account for this to work
# see instructions here
# https://docs.ropensci.org/deposits/articles/install-setup.html#setup-api-tokens
# you can also set it in your r environment

# Sys.setenv("FIGSHARE_TOKEN" = "your token here")
cli <- depositsClient$new("figshare")
cli$deposit_retrieve(25308250)

file_list <- cli$hostdata$files
print(file_list$name)

files <- file_list$name

map(files, \(x) {
  cli$deposit_download_file(x, path = data_loc, overwrite = T)
})

# count data
data <- here::here(data_loc, "mamu-count-data.csv") %>%
  read_csv()


schema <- arrow::read_parquet(
  here::here("data", "cleaned", "salamanders_BQ_format.parquet")
) %>%
  names()

taxa_info <- taxize::classification(
  "Brachyramphus marmoratus",
  db = "gbif",
  rows = 1
) %>%
  rbind() %>%
  select(-id) %>%
  pivot_wider(names_from = rank, values_from = name) %>%
  mutate(vernacular = "Marbled Murrelet") %>%
  select(-query) %>%
  rename(valid_scientific_name = species)

data %>%
  bind_cols(taxa_info)

organized <- data %>%
  mutate(
    start_dt = make_datetime(Year, Month, Day, StartHour, StartMinute),
    # using survey duration to calculate the ending time as sometime it is NA
    end_dt = start_dt + minutes(SurveyDuration),
    meantime = start_dt + (end_dt - start_dt) / 2
  ) %>%
  mutate(
    year_obs = Year,
    month_obs = Month,
    day_obs = Day,
    time_obs = time(meantime)
  ) %>%
  # there are justifications for using other numbers for the values
  # i am choosing to use the total incoming and outgoing.
  # the metadata is not clear on if MamuIn includes the predawn values
  mutate(
    observation_value = MamuIn + MamuOut,
    observation_type = "count",
    effort_sampling_value = NA,
    effort_sampling_method = "radar counts",
    effort_sampling_unit = NA
  ) %>%
  rename(longitude = Lon, latitude = Lat) %>%
  mutate(
    coordinate_uncertainty = Radius * 1852, # nautical miles to meters
    coordinate_uncertainty_unit = "m"
  ) %>%
  mutate(
    dataset_name = "Data from marine radar counts of marbled murrelets (Brachyramphus marmoratus) in British Columbia, Canada",
    dataset_creator = "Doug Bertram",
    dataset_publisher = "Figshare",
    dataset_url_information = "https://figshare.com/articles/dataset/Data_from_marine_radar_counts_of_marbled_murrelets_i_Brachyramphus_marmoratus_i_in_British_Columbia_Canada/25308250/1",
    dataset_url_download = "https://figshare.com/ndownloader/articles/25308250/versions/1",
    dataset_doi = "https://doi.org/10.6084/m9.figshare.25308250.v1",
    license = "CC-BY-4.0"
  )

cleaned <- organized %>%
  bind_cols(taxa_info) %>%
  mutate(group = "birds", observed_rank = "species") %>%
  select(all_of(schema))

save_loc <- here::here(
  "data",
  "cleaned",
  "murrelet_radar_counts.parquet"
)
fs::dir_create(dirname(save_loc))

arrow::write_parquet(cleaned, save_loc)
