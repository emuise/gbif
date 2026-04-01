library(tidyverse)
library(dataone)
library(bcmaps)
library(terra)

# write('CURL_SSL_BACKEND=openssl', file = "~/.Renviron", append = TRUE)

mysearch = list(attribute = "longitude", rows = "1000")

solr <- list(q="id:doi*", rows="10000", fq="attributeName:longitude", fl="id,title,dateUploaded,abstract,datasource,size")


cn <- CNode("PROD")

sResult <- query(cn, solrQuery = solr, as = "data.frame")
result <- query(cn, searchTerms = mysearch, as = "data.frame")


library(dataone)

cn <- CNode("PROD")

# Query for all records (q="*:*") but return only facet counts
facet_query <- list(
  q = "*:*",
  rows = "10000",               # We don't need the actual documents, just the list
  facet = "true",
  facet.field = "attributeName",
  facet.limit = "-1",       # -1 returns all unique attributes regardless of count
  facet.mincount = "1"      # Only return attributes that appear at least once
)

# Execute query
# Get the raw list output
raw_result <- dataone::query(cn, solrQuery = facet_query, as = "list")

atts <- map(raw_result, \(x) {

  if(is.null(x$attribute)) {
    return()
  }

  x$attributeName %>%
    unlist()


}) %>%
  unlist()


as_tibble(atts) %>%
  count(value) %>%
  arrange(desc(n))




library(dataone)

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

overlap_result %>%
  filter_out(str_detect(title, "IPCC")) %>%
  filter_out(str_detect(title, "Photogrammetric"))
