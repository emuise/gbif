library(tidyverse)
library(dataone)
# windows workaround for an error in dataone as of april 14, 2024
# write('CURL_SSL_BACKEND=openssl', file = "~/.Renviron", append = TRUE)
library(terra)
library(tidyterra)
library(taxize)
data_loc <- here::here("data", "raw_data", "dataone", "gpdd")
fs::dir_create(data_loc)

cn <- CNode("PROD")
mn <- getMNode(cn, "urn:node:KNB")


# Define the identifier
packageId <- "doi:10.5063/F1BZ63Z8"

# Download the package
pkg <- getPackage(mn, packageId)

# unfortunately the data isn't nicely named and there isnt an easy way to attach that info
unzip(pkg, exdir = data_loc)

locations <- here::here(data_loc, "data", "df35b.239.1-DATA.csv") %>% read_csv()

bcb <- bcmaps::bc_bound() %>% vect() %>%
  mutate(agg = "agg") %>%
  aggregate("agg")

bcb_4326 <- bcb %>% project("epsg:4326")

spatial_locs <- locations %>%
  vect(geom = c("LongDD", "LatDD"), crs = "epsg:4326") %>%
  project("epsg:3005")

# Assuming 'locations' is a dataframe with columns: North, South, East, West
all_bbox <- locations %>%
  filter(!is.na(North), !is.na(South), !is.na(East), !is.na(West)) %>%
  pmap(\(North, South, East, West, ...) {
    ext(
      min(West, East),
      max(West, East),
      min(South, North),
      max(South, North)
    ) %>%
      vect(crs = "epsg:4326")
  }) %>%
  vect()

values(all_bbox) <- locations %>%
  filter(!is.na(North), !is.na(South), !is.na(East), !is.na(West))

# i like this map
ggplot() +
  geom_spatvector(data = all_bbox, aes(fill = Continent), alpha = 0.2, colour = "transparent") +
  theme_void() +
 theme(legend.position = "none") 


overlap_ind <- relate(bcb_4326, all_bbox, "overlaps") %>%
  as.logical()

# smallest area is 100000 ha. so not useful unfortunately.
all_bbox[overlap_ind, ] %>%
  project("epsg:3005") %>%
  expanse(unit = "ha") %>%
  sort()

message("unfortunately the gppd doesn't have useful data in it.")