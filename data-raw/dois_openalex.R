# Workflow that retrieves California Department of Water Resources authored
# or funded works using the OpenAlex functional helpers and then cleans and
# saves DOIS

load(here::here("data", "dois_drop.rda"))

dwr_pattern <- "California Department of Water Resources"
from_date <- "2020-01-01"

oa_funder_matches <- oa_lookup_funder_ids(dwr_pattern, per_page = 1)

if (!length(oa_funder_matches)) {
  stop(
    "Unable to determine OpenAlex funder ID for DWR via API lookup. ",
    "Please update the query or provide the ID manually."
  )
}

oa_dwr_funder_id <- sub("^.*/", "", oa_funder_matches[[1]])

oa_dwr_author_filter <- function(works) {
  oa_filter_authorships(works, pattern = dwr_pattern)
}

oa_strategies <- tibble::tibble(
  fetch = list(
    list(
      raw_affiliation_strings.search = dwr_pattern,
      from_publication_date = from_date,
      type = "article"
    ),
    list(
      grants.funder = oa_dwr_funder_id,
      from_publication_date = from_date,
      type = "article"
    )
  ),
  filter = list(oa_dwr_author_filter, identity),
  post = list(identity, identity)
)

oa_dwr_works <- oa_run_strategies(oa_strategies)

dois_openalex <- oa_extract_dois(oa_dwr_works)
dois_openalex <- dois_openalex[!dois_openalex %in% dois_drop]

usethis::use_data(dois_openalex, overwrite = TRUE)
