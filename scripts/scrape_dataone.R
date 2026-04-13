library(tidyverse)
library(dataone)
library(bcmaps)
library(terra)

bc_ext <- bcmaps::bc_bound() %>%
  vect() %>%
  project("epsg:4326") %>%
  ext() %>%
  as.vector() %>%
  round(5)

cn <- CNode("PROD")

# Overlap logic:
# [Target Min TO *] finds everything starting at or above your bottom edge
# [* TO Target Max] finds everything ending at or below your top edge
# this includes anything that intersects bc, including global datasets
overlap_filter <- glue::glue(
  "northBoundCoord:[{bc_ext['ymin']} TO *] AND ",
  "southBoundCoord:[* TO {bc_ext['ymax']}] AND ",
  "eastBoundCoord:[{bc_ext['xmin']} TO *] AND ",
  "westBoundCoord:[* TO {bc_ext['xmax']}]"
)

solr_params <- list(
  q = "id:doi*",
  fq = overlap_filter,
  rows = "10000"
)

overlap_result <- dataone::query(cn, solrQuery = solr_params, as = "data.frame")


output2 <- overlap_result %>%
  filter_out(str_detect(str_to_lower(title), "ipcc")) %>%
  filter_out(str_detect(str_to_lower(title), "hotogramm")) %>%
  filter_out(str_detect(str_to_lower(title), "physical oceanography")) %>%
  filter_out(str_detect(str_to_lower(title), "arcticRIMS")) %>%
  filter_out(str_detect(str_to_lower(title), "arche")) %>%
  filter_out(str_detect(str_to_lower(title), "archaeol")) %>%
  filter_out(str_detect(str_to_lower(title), "sediment core")) %>%
  filter_out(str_detect(str_to_lower(title), "radiocarbon")) %>%
  filter_out(str_detect(str_to_lower(title), "weather")) %>%
  filter_out(str_detect(str_to_lower(title), "iceberg")) %>%
  filter_out(str_detect(str_to_lower(title), "glacier")) %>%
  filter_out(str_detect(str_to_lower(title), "alaska")) %>% # this could be an error to exclude, i am excluding on the assumption that nothing cross border was done. there is species data on salmon in this.
  filter_out(str_detect(str_to_lower(title), "aws")) %>%
  filter_out(str_detect(str_to_lower(title), "bering")) %>%
  filter_out(str_detect(str_to_lower(title), "ice")) %>%
  filter_out(str_detect(str_to_lower(title), "isotope")) %>%
  filter_out(str_detect(str_to_lower(title), "tide")) %>%
  filter_out(str_detect(str_to_lower(title), "snow")) %>%
  filter_out(str_detect(str_to_lower(title), "water ")) %>% #SPACE IS NEEDED THERE IS A WATERFOWL POPULATION DATASET
  filter_out(str_detect(str_to_lower(title), "carbon")) %>%
  filter_out(str_detect(str_to_lower(title), "arctic")) %>%
  filter_out(str_detect(str_to_lower(title), "seward")) %>%
  filter_out(str_detect(str_to_lower(title), "chemistry")) %>%
  filter_out(str_detect(str_to_lower(title), "bathymetry")) %>%
  filter_out(str_detect(str_to_lower(title), "satellite")) %>%
  filter_out(str_detect(str_to_lower(title), "protein")) %>%
  filter_out(str_detect(str_to_lower(title), "ncep")) %>%
  filter_out(str_detect(str_to_lower(title), "modis")) %>%
  filter_out(str_detect(str_to_lower(title), "site temperature data")) %>%
  filter_out(str_detect(str_to_lower(title), "hydro1k")) %>%
  filter_out(str_detect(str_to_lower(title), "hourly")) %>%
  filter_out(str_detect(str_to_lower(title), "cumulative")) %>%
  filter_out(str_detect(str_to_lower(title), "noaa")) %>%
  filter_out(str_detect(str_to_lower(title), "gravity")) %>%
  filter_out(str_detect(str_to_lower(title), "vertnet")) %>% # included in the gbif
  filter_out(str_detect(str_to_lower(title), "vegbank")) %>% # included in the gbif
  filter_out(str_detect(str_to_lower(title), "usa")) %>%
  filter_out(str_detect(str_to_lower(title), "hly")) %>% #US coast guard boat
  #filter_out(str_detect(str_to_lower(title), "usda")) %>%
  filter(is.na(obsoletedBy)) %>%
  select(
    id,
    title,
    keywords,
    project,
    obsoletedBy,
    isPublic,
    dataUrl,
    abstract,
    attributeName,
    kingdom,
    phylum,
    class,
    order,
    family,
    genus,
    species
  ) %>%
  arrange(title) # %>%
filter(str_detect(str_to_lower(title), "usda"))

output2 %>%
  pull(title)

write_csv(output2, "overlap.csv")

output2 %>%
  pull(keywords)

# this checks if the data has "deny first permissions"
# meaning its just information that the data exists, not the data itself
output2 %>%
  mutate(
    access = map(dataUrl, \(x) {
      #page <- RCurl::getURL(x)

      lines <- tryCatch(
        {
          readLines(x)
        },
        error = function(msg) {
          return(NA)
        }
      )
      if(all(is.na(lines))) {
        return(NA)
      }

      deny <- str_subset(lines, "denyFirst")

      if (length(deny) == 1) {
        return(F)
      }
      T
    }, .progress = T)
  )

accessed <- as_tibble(save) %>% unnest(access) %>% filter(is.na(access) | access) %>% filter_out(str_detect(str_to_lower(title), "hly"))

write_csv(accessed, "output2.csv")
