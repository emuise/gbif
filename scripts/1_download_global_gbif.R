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

run_download <- function() {
  download_flag <- here::here("flags", "download_done.txt")

  if (fs::file_exists(download_flag)) {
    message("Download already marked as complete. Skipping.")
    return(invisible(NULL))
  }

  message("Starting GBIF download...")
  # pak::pak("minioclient")
  # minioclient::install_mc()
  gbifdb::gbif_download(dir = raw_gbif_loc)

  fs::dir_create(dirname(download_flag))
  fs::file_create(download_flag)
}

run_download()


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



run_split <- function() {
  split_flag <- here::here("flags", "split_done.txt")

  if (fs::file_exists(split_flag)) {
    message("Split already marked as complete. Skipping.")
    return(invisible(NULL))
  }

  dones <- fs::dir_ls(scratch_dir)
  files <- fs::dir_ls(raw_gbif_loc, recurse = TRUE, type = "file")
  files_left <- files[!(basename(files) %in% basename(dones))]

  walk(files_left, \(file) {
    tempfile <- here::here("scratch", "noram_process", basename(file))
    if (fs::file_exists(tempfile)) {
      return()
    }
    message(paste0("Processing file ", file, " of ", length(files), "..."))

    filtered_df <- file %>%
      open_dataset() %>%
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

    filtered_df <- filtered_df %>%
      filter(!purrr::map_lgl(issue, ~ any(.x$array_element %in% geo_issues)))

    if (nrow(filtered_df) > 0) {
      write_dataset(
        filtered_df,
        path = clean_dir,
        format = "parquet",
        partitioning = c("kingdom", "family"),
        existing_data_behavior = "overwrite",
        max_partitions = 20000,
        basename_template = paste0("part-", basename(file), "-{i}.parquet")
      )
    }

    fs::dir_create(dirname(tempfile))
    fs::file_create(tempfile)

    rm(filtered_df)
    gc(full = TRUE)
    gc(full = TRUE)
  })

  if (fs::dir_exists(raw_gbif_loc)) {
    fs::dir_delete(raw_gbif_loc)
  }

  fs::dir_create(dirname(split_flag))
  fs::file_create(split_flag)
}

run_split()


condense_fold_name <- "gbif_noram2"

condensed_dir <- here::here("data", condense_fold_name)

run_compaction <- function() {
  compact_flag <- here::here("flags", "compaction_done.txt")

  # Guard clause: stop if already done
  if (fs::file_exists(compact_flag)) {
    message("Compaction already marked as complete. Skipping.")
    return(invisible(NULL))
  }

  l1 <- fs::dir_ls(clean_dir, recurse = FALSE, type = "dir")

  message("condensing directory")
  walk(l1, \(x) {
    l2 <- fs::dir_ls(x, type = "dir")
    walk(l2, \(y) {
      dirname <- str_replace(y, "gbif_noram", condense_fold_name)
      if (fs::dir_exists(dirname)) {
        message("deleting completed dir ", y)
        fs::dir_delete(y)
        return()
      }

      message(paste0("processing ", dirname))
      fs::dir_create(dirname)

      files <- fs::dir_ls(y, type = "file")
      pq <- open_dataset(files)
      write_dataset(
        pq,
        path = dirname,
        format = "parquet",
        existing_data_behavior = "overwrite",
        max_rows_per_file = 1000000,
        max_partitions = 20000
      )
      message("deleting completed dir ", y)
      fs::dir_delete(y)
      gc(full = TRUE)
    })
  })

  gc(full = TRUE)
  # walk back through to delete the files in the directories

  message("files condensed, cleaning up uncompacted files")
  path <- normalizePath(clean_dir, mustWork = FALSE)

  if (.Platform$OS.type == "windows") {
    # /s removes all subdirectories and files; /q runs in quiet mode (no prompt)
    system(
      sprintf('cmd.exe /c rmdir /s /q "%s"', path),
      show.output.on.console = FALSE
    )
  } else {
    # Unix/macOS equivalent
    system(sprintf('rm -rf "%s"', path))
  }

  message("moving condensed files to clean dir")
  fs::dir_delete(clean_dir)
  fs::file_move(condensed_dir, clean_dir)
  fs::dir_create(dirname(compact_flag))
  fs::file_create(compact_flag)
}

run_compaction()


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
  c_files,
  \(x) {
    kingdom <- dirname(x) %>%
      dirname() %>%
      basename() %>%
      str_remove("kingdom=")
    family <- dirname(x) %>% basename() %>% str_remove("family=")

    savename <- here::here(
      spatcount_dir,
      glue::glue("{kingdom}_{family}_{basename(x)}")
    )
    if (file.exists(savename)) {
      return()
    }

    message(paste("Counting", x))

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
      )

    arrow::write_parquet(inout, savename)
  },
  .progress = TRUE
)
