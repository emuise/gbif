library(gbifdb)
library(arrow)
library(tidyverse)
library(countrycode)
library(geodata)
library(terra)
library(tidyterra)
library(sf) # fast intersect

raw_gbif_loc <- here::here("data", "gbif_global")

scratch_dir <- here::here("scratch", "noram_process")
clean_dir <- here::here("data", "gbif_noram")

fs::dir_create(scratch_dir)
fs::dir_create(clean_dir)

run_download <- function(max_retries = Inf) {
  download_flag <- here::here("flags", "download_done.txt")

  if (fs::file_exists(download_flag)) {
    message("Download already marked as complete. Skipping.")
    return(invisible(NULL))
  }

  # minioclient::install_mc()
  attempt <- 1
  while (!fs::file_exists(download_flag) && attempt <= max_retries) {
    message(sprintf("Starting GBIF download (Attempt %d)...", attempt))

    tryCatch(
      {
        gbifdb::gbif_download(dir = raw_gbif_loc)

        fs::dir_create(dirname(download_flag))
        fs::file_create(download_flag)
        message("Download complete.")
      },
      error = function(e) {
        message(sprintf(
          "Download failed on attempt %d: %s",
          attempt,
          e$message
        ))
        message("Retrying in 5 seconds...")
        Sys.sleep(5)
      }
    )

    attempt <- attempt + 1
  }
}

run_download(max_retries = 30)

if (!file.exists(here::here("flags", "download_done.txt"))) {
  gbif_files <- fs::dir_ls(raw_gbif_loc, recurse = T, type = "file")
  fs::file_delete(gbif_files[is.na(as.numeric(basename(gbif_files)))])
}

noram <- vect(
  "https://github.com/gbif/continents/raw/master/continent_cookie_cutter.gpkg"
) %>%
  filter(continent_part == "north_america_east")

world <- geodata::world(path = here::here("data", "shps")) %>%
  project(noram)

na_countries <- intersect(world, noram)
na_bbox <- ext(na_countries)

na_cc <- na_countries %>% pull(GID_0)
na_cc_iso2c <- countrycode(na_cc, origin = "iso3c", destination = "iso2c")

ymin <- na_bbox$ymin
ymax <- na_bbox$ymax
xmin <- na_bbox$xmin
xmax <- na_bbox$xmax

geo_issues <- c(
  "COORDINATE_REPROJECTION_FAILED",
  "COORDINATE_REPROJECTION_SUSPICIOUS",
  "COORDINATE_UNCERTAINTY_METERS_INVALID",
  "PRESUMED_NEGATED_LATITUDE",
  "FOOTPRINT_WKT_MISMATCH",
  "FOOTPRINT_WKT_INVALID",
  "COUNTRY_COORDINATE_MISMATCH",
  "COORDINATE_PRECISION_INVALID",
  "PRESUMED_NEGATED_LONGITUDE",
  "CONTINENT_COUNTRY_MISMATCH",
  "CONTINENT_COORDINATE_MISMATCH",
  "PRESUMED_SWAPPED_COORDINATE"
)

run_split_grouped <- function() {
  split_flag <- here::here("flags", "split_done.txt")

  if (fs::file_exists(split_flag)) {
    message("Split already marked as complete. Skipping.")
    return(invisible(NULL))
  }

  dones <- fs::dir_ls(scratch_dir)
  files <- fs::dir_ls(raw_gbif_loc, recurse = TRUE, type = "file")
  files_left <- files[!(basename(files) %in% basename(dones))]

  glob_max <- files_left %>% basename() %>% as.numeric() %>% max()

  groups <- split(files_left, ceiling(seq_along(files_left) / 10))

  walk(groups, \(group) {
    tempfiles <- here::here("flags", "noram_process", basename(group))

    remaining <- group[!fs::file_exists(tempfiles)]

    if (length(remaining) == 0) {
      return()
    }

    nums <- remaining %>%
      basename() %>%
      as.numeric()

    template <- glue::glue("{min(nums)}-{max(nums)}")

    message(glue::glue(
      "\n=== Processing Files [{template}] (Max: {glob_max}) ==="
    ))

    df <- remaining %>%
      open_dataset()

    message(glue::glue(
      "  │ Raw records loaded : {format(nrow(df), big.mark = ',')}"
    ))

    filtered_df <- df %>%
      filter(
        occurrencestatus == "PRESENT",
        !basisofrecord %in% c("FOSSIL_SPECIMEN", "LIVING_SPECIMEN"),
        !is.na(species),
        !is.na(decimallatitude),
        !is.na(decimallongitude),
        countrycode %in% na_cc_iso2c | is.na(countrycode),
        decimallatitude < ymax,
        decimallatitude > ymin,
        decimallongitude < xmax,
        decimallongitude > xmin
      ) %>%
      collect()

    message(glue::glue(
      "  │ Post spatial filter: {format(nrow(filtered_df), big.mark = ',')}"
    ))

    filtered_df <- filtered_df %>%
      filter(!purrr::map_lgl(issue, ~ any(.x$array_element %in% geo_issues)))

    message(glue::glue(
      "  │ Post issue filter  : {format(nrow(filtered_df), big.mark = ',')}"
    ))

    if (nrow(filtered_df) > 0) {
      message("  └─ Saving Parquet... ")
      write_dataset(
        filtered_df,
        path = clean_dir,
        format = "parquet",
        existing_data_behavior = "overwrite",
        max_partitions = 20000,
        max_rows_per_file = 1000000,
        basename_template = paste0("part_", template, "_{i}.parquet")
      )
    } else {
      message("  └─ No rows remaining. Skipping write.")
    }

    fs::dir_create(dirname(tempfiles)[[1]])
    fs::file_create(tempfiles)

    rm(filtered_df)
    gc(full = TRUE)
    gc(full = TRUE)
  })

  fs::dir_create(dirname(split_flag))
  fs::file_create(split_flag)
}

run_split_grouped()

if (fs::dir_exists(raw_gbif_loc)) {
  fs::dir_delete(raw_gbif_loc)
}


# now that the files are condesned, listing them is relaitvely quick
# we actually want to operate on a file bases, and can condense the output at the end
# so basically, birds are going to be unable to process all at once
# i.e. geese have > 100 million observations
# every other family is /reasonably large/
# and can be done at once

# operate on condensed files
c_files <- fs::dir_ls(clean_dir, recurse = T, type = "file")

bcb <- bcmaps::bc_bound_hres() %>%
  vect()

bcb_sf <- bcb %>%
  st_as_sf()

bcb_bbox <- bcb %>%
  project("epsg:4326") %>%
  ext()

spatcount_dir <- here::here("data", "spatial_counts")
fs::dir_create(spatcount_dir)

walk(
  1:length(c_files),
  \(n) {
    x <- c_files[n]
    savename <- here::here(
      spatcount_dir,
      glue::glue("{basename(x)}")
    )
    if (file.exists(savename)) {
      return()
    }

    message(glue::glue("[{n}/{length(c_files)}]: {basename(x)}"))

    df <- arrow::open_dataset(x) %>%
      mutate(kingdom = kingdom, family = family) %>%
      select(
        kingdom,
        phylum,
        class,
        order,
        family,
        genus,
        species,
        decimallatitude,
        decimallongitude
      ) %>%
      collect()
    # index of if points are in bounding box
    in_bbox <- df$decimallatitude <= bcb_bbox$ymax &
      df$decimallatitude >= bcb_bbox$ymin &
      df$decimallongitude <= bcb_bbox$xmax &
      df$decimallongitude >= bcb_bbox$xmin

    # static integer column
    df$in_bc <- 0L

    # if any are in the bounding box, do a spatial intersect to see if they are in the BC polygon
    if (any(in_bbox)) {
      # sf is fastest for these intersections, keeping the polygon in it's normal crs is also fastest
      # so we project the points from wgs84 into bc albers for speed
      # we dont need to reproject out or anything
      spat <- vect(
        df[in_bbox, ],
        geom = c("decimallongitude", "decimallatitude"),
        crs = "epsg:4326"
      ) %>%
        project(bcb) %>%
        st_as_sf()

      inters <- st_intersects(spat, bcb_sf, sparse = T)

      # if number of intersects greater than 0, it is within the bounding box of bc
      df$in_bc[in_bbox] <- as.integer(lengths(inters) > 0)
    }

    # group by summarize if it is inside or outside bc
    inout <- df %>%
      group_by(kingdom, phylum, class, order, family, genus, species) %>%
      summarize(
        n_bc = sum(in_bc == 1L),
        n_notbc = sum(in_bc == 0L),
        n_total = n(),
        .groups = "drop"
      ) %>%
      mutate(file = x)

    arrow::write_parquet(inout, savename)
  }
)

counts_wfiles <- arrow::open_dataset(spatcount_dir)

all_counts <- counts_wfiles %>%
  group_by(kingdom, phylum, class, order, family, genus, species) %>%
  summarize(
    n_bc = sum(n_bc),
    n_notbc = sum(n_notbc),
    n_total = sum(n_total)
  )

bc_counts <- all_counts %>%
  filter(n_bc > 0) %>%
  collect() %>%
  arrange(kingdom, phylum, class, order, family, genus, species)

species_list <- bc_counts %>%
  pull(species)

rast_vars <- c(
  "MAP", # mean annual precipitation (mm)
  "DD_0", # chilling degree days (Degree days below 0 °C)
  "PAS", # precipitation as snow (mm)
  "CMD", # Hargreave’s climatic moisture index
  "DD18"
) # warming degree days above 18 °C.

# this is downloaded in the julia file
# i have double checked that all climate layers use the same mask
snap <- here::here("data", "climate", "Normal_1991_2020_bioclim") %>%
  fs::dir_ls(type = "file") %>%
  .[[1]] %>%
  rast()

snap_vals <- terra::values(snap)[, 1]

cellcount_dir <- here::here("data", "cellcounts")
fs::dir_create(cellcount_dir)

walk(1:length(c_files), \(n) {
  x <- c_files[[n]]
  savename <- here::here(cellcount_dir, basename(x))
  if (file.exists(savename)) {
    return()
  }

  message(glue::glue("[{n}/{length(c_files)}]: {basename(x)}"))

  df <- open_dataset(x) %>%
    select(species, decimallatitude, decimallongitude) %>%
    collect()

  if (nrow(df) == 0) {
    # probably should save an empty file
    return()
  }

  pts <- terra::vect(
    df,
    geom = c("decimallongitude", "decimallatitude"),
    crs = "epsg:4326"
  ) %>%
    project(snap)

  df$cell <- terra::cellFromXY(snap, terra::crds(pts))

  cc <- df %>%
    filter(!is.na(cell)) %>% # this removes anything outside the raster bounds
    mutate(snap_val = snap_vals[cell]) %>%
    filter(!is.na(snap_val)) %>%
    select(-snap_val) %>%
    count(species, cell, name = "n") %>%
    arrow::write_parquet(savename)
})

cc <- arrow::open_dataset(cellcount_dir)

cellcounts <- cc %>%
  group_by(species, cell) %>% 
  summarise(n = sum(n))

sample_intensity <- cc %>%
  group_by(cell) %>%
  summarize(n = sum(n)) %>%
  collect()

r <- snap
values(r) <- NA

r[!is.nan(snap_vals)] <- 0

r[sample_intensity$cell] <- sample_intensity$n

smooth_r <- focal(r, w = 15, fun = "mean", na.rm = T, na.policy = "omit")

plot(log(smooth_r), main = "log(mean sampling intensity) (30 km focal window)")
