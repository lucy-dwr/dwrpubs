# Workflow that retrieves Crossref metadata for California Department of Water
# Resources (DWR) authored or funded works using the functional helpers.

dwr_pattern <- "California Department of Water Resources"
from_date <- "2020-01-01"

cr_funder_matches <- cr_lookup_funder_ids(dwr_pattern, limit = 1)

if (!length(cr_funder_matches)) {
  stop(
    "Unable to determine Crossref funder ID for DWR via API lookup. ",
    "Please update the query or provide the ID manually."
  )
}

dwr_funder_id <- cr_funder_matches[[1]]

dwr_affiliation_filter <- function(works) {
  cr_filter_affiliations(works, pattern = dwr_pattern)
}

cr_strategies <- tibble::tibble(
  fetch = list(
    list(
      `query.affiliation` = dwr_pattern,
      filters = list(
        `from-pub-date` = from_date
      )
    ),
    list(
      filters = list(
        funder = dwr_funder_id,
        `from-pub-date` = from_date
      )
    )
  ),
  filter = list(dwr_affiliation_filter, identity),
  post = list(identity, identity)
)

cr_dwr_works <- cr_run_strategies(cr_strategies)

dois_crossref <- cr_extract_dois(cr_dwr_works)

usethis::use_data(dois_crossref, overwrite = TRUE)
