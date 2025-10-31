# ---
# Purpose: Build the final inventory by augmenting the classified inventory
# with denormalised author strings and formatted citations.
# ---

source(here::here("R", "final_inventory_helpers.R"))
load(here::here("data", "classified_inventory.rda"))

final_inventory <- classified_inventory |>
  dplyr::mutate(
    authors_collapsed = purrr::map_chr(authors, derive_author_string),
    affiliations_collapsed = purrr::map_chr(author_affiliations, derive_affiliation_string),
    dwr_contribution_collapsed = summarise_dwr_contribution(
      dwr_sole_author,
      dwr_lead_author,
      dwr_author,
      dwr_funding_other
    )
  ) |>
  dplyr::mutate(
    citation = purrr::pmap_chr(
      list(
        authors = authors,
        publication_year = publication_year,
        title = title,
        journal = journal,
        volume = volume,
        issue = issue,
        first_page = first_page,
        last_page = last_page,
        doi = doi
      ),
      format_harvard_citation
    )
  ) |>
  dplyr::mutate(
    dplyr::across(
      c(
        title,
        citation,
        authors_collapsed,
        affiliations_collapsed,
        journal,
        publisher,
        note
      ),
      sanitize_text
    )
  )

final_inventory <- final_inventory|>
  dplyr::select(
    doi,
    title,
    citation,
    authors_collapsed,
    affiliations_collapsed,
    dwr_contribution_collapsed,
    first_level,
    second_level,
    confidence,
    abstract,
    note,
    journal,
    publication_year,
    volume,
    issue,
    publisher,
    landing_page_url,
    pdf_url,
    is_oa,
    is_oa_anywhere,
    oa_status,
    license,
    cited_by_count,
    dwr_author,
    dwr_lead_author,
    dwr_sole_author,
    dwr_funding_other,
    first_dwr_author,
    first_dwr_author_position,
    first_dwr_author_division
  )

readr::write_csv(final_inventory,
  file = here::here("output", "final_inventory.csv"),
  na = ""
)

usethis::use_data(final_inventory, overwrite = TRUE)
