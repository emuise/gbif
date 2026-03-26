library(arrow)
library(tidyverse)
library(terra)
library(tidyterra)
library(bcmaps)
library(taxize)

# data
species <- "Rangifer tarandus"
common_name <- "Caribou"

taxa <- classification(species, db = "gbif") %>%
  rbind() %>%
  select(-id) %>%
  pivot_wider(names_from = rank, values_from = name)

bcb <- bc_bound_hres()
bcb_v <- vect(bcb)
bcb_l <- bc_bound() %>%
  vect()

occ <- here::here("data", "gbif_hive") %>%
  arrow::open_dataset() %>%
  dplyr::filter(class == taxa$class, species == taxa$species) %>%
  collect()

occ_sp <- st_as_sf(
  occ,
  coords = c("decimallongitude", "decimallatitude"),
  crs = 4326
) %>%
  st_transform(3005)

occ_bc <- occ_sp %>%
  mutate(
    intersect = st_intersects(occ_sp, bcb) %>%
      as.logical()
  ) %>%
  filter(intersect) %>%
  select(-intersect) %>%
  vect()
plot(bcb_v)
plot(occ_bc, add = T)

grid <- st_make_grid(bcb, n = c(20, 20), square = F) %>%
  st_as_sf() %>%
  mutate(
    intersect = st_intersects(., bcb) %>%
      as.logical()
  ) %>%
  filter(intersect) %>%
  select(-intersect) %>%
  mutate(gridno = row_number()) %>%
  vect()

plot(bcb_l)
plot(grid, add = T)
plot(occ_bc, add = T)

species_counts <- intersect(grid, occ_bc) %>%
  count(gridno) %>%
  as_tibble()


grid_counts <- left_join(grid, species_counts)

ggplot() +
  geom_spatvector(data = grid_counts, aes(fill = n)) +
  geom_spatvector(data = bcb_l, fill = "#00000000") +
  theme_void() +
  labs(
    title = glue::glue("GBIF Occurances for {common_name} (*{species}*)"),
    fill = "Number of Occurances"
  ) +
  theme(legend.position = "bottom", legend.title.position = "top") +
  scale_fill_viridis_c()

library(basemaps)
set_defaults(map_service = "osm", map_type = "topographic")