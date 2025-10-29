# ---
# Purpose: Aggregate DWR metadata from Crossref and OpenAlex using DOIs,
# annotate contributions, and store the harmonized records as package data.
# ---

load(here::here("data", "dois_manual.rda"))
load(here::here("data", "dois_crossref.rda"))
load(here::here("data", "dois_openalex.rda"))

dwr_pattern <- "California Department of Water Resources"
new_affiliation_lookup <- FALSE

# build a DOI registry from all sources
dwr_doi_sources <- build_doi_registry(
  manual = dois_manual,
  crossref = dois_crossref,
  openalex = dois_openalex
)

# import metadata from Crossref
crossref_metadata <- fetch_crossref_metadata(dwr_doi_sources$doi) |>
  dplyr::select(-'na.', -'na..1') |>
  dplyr::mutate(
    doi = clean_doi(doi),
    publication_year = ifelse(
      !is.na(published.online),
      substr(published.online, 1, 4),
      substr(created, 1, 4)
    )
  ) |>
  dplyr::filter(!is.na(doi), publication_year >= 2020) |>
  dplyr::mutate(
    abstract = clean_jats_abstract(abstract),
    author = format_crossref_authors(author)
  ) |>
  dplyr::left_join(dwr_doi_sources, by = "doi") |>
  dplyr::mutate(
    authors = purrr::map(author, extract_crossref_author_names)
  ) |>
  detect_authorship(
    pattern = dwr_pattern,
    source = "crossref",
    flag_column = "dwr_author",
    authorship_type = c("any", "lead", "sole")
  ) |>
  detect_funding(
    pattern = dwr_pattern,
    source = "crossref",
    flag_column = "dwr_funding_other"
  ) |>
  clean_article_titles()

# import metadata from OpenAlex
openalex_metadata <- fetch_openalex_metadata(dwr_doi_sources$doi) |>
  dplyr::mutate(doi = clean_doi(doi)) |>
  dplyr::filter(!is.na(doi), publication_year >= 2020) |>
  dplyr::left_join(dwr_doi_sources, by = "doi") |>
  dplyr::mutate(
    unique_affiliations = purrr::map(
      authorships,
      extract_openalex_affiliations
    )
  ) |>
  detect_authorship(
    pattern = dwr_pattern,
    source = "openalex",
    flag_column = "dwr_author",
    authorship_type = c("any", "lead", "sole")
  ) |>
  detect_funding(
    pattern = "Department of Water Resources",
    source = "openalex",
    flag_column = "dwr_funding_other"
  ) |>
  clean_article_titles()

# canonicalize affiliation names in OpenAlex metadata using LLM if
# indicated, or load a pre-generated affiliation lookup table
if (new_affiliation_lookup) {
  affiliation_lookup <- canonicalize_affiliation_values(
    openalex_metadata$unique_affiliations,
    model = "gemini-2.5-flash-lite",
    base_url = "https://generativelanguage.googleapis.com/v1beta/",
    api_key = Sys.getenv("GOOGLE_API_KEY"),
    batch_size = 50,
    group_size = 20,
    max_active = 8,
    rpm = 120,
    parallel_retries = 2,
    retry_backoff = 3,
    verbose = interactive()
  )
} else {
  load(here::here("data", "affiliation_lookup.rda"))
}

# annotate OpenAlex metadata with canonical affiliations and extracted
# author names
openalex_metadata <- openalex_metadata |>
  dplyr::mutate(
    canonical_affiliations = purrr::map(
      unique_affiliations,
      map_canonical_affiliations,
      lookup = affiliation_lookup
    ),
    authors = purrr::map(authorships, extract_openalex_author_names),
    author_affiliations = purrr::map(
      authorships,
      build_openalex_author_affiliations,
      lookup = affiliation_lookup
    )
  )

# harmonize Crossref and OpenAlex metadata
openalex_publisher <- if ("publisher" %in% names(openalex_metadata)) {
  openalex_metadata$publisher
} else {
  rep(NA_character_, nrow(openalex_metadata))
}

openalex_subset <- openalex_metadata |>
  dplyr::mutate(
    publication_year_openalex = as.integer(publication_year),
    cited_by_count_openalex = as.integer(cited_by_count),
    publisher_openalex = openalex_publisher
  ) |>
  dplyr::select(
    doi,
    title_openalex = title,
    abstract_openalex = abstract,
    authors_openalex = authors,
    canonical_affiliations,
    journal_openalex = source_display_name,
    publication_year_openalex,
    is_oa,
    is_oa_anywhere,
    oa_status,
    landing_page_url_openalex = landing_page_url,
    pdf_url,
    license,
    cited_by_count_openalex,
    first_page,
    last_page,
    volume_openalex = volume,
    issue,
    publisher_openalex,
    doi_source_openalex = doi_source,
    author_affiliations,
    dwr_author_openalex = dwr_author,
    dwr_lead_author_openalex = dwr_lead_author,
    dwr_sole_author_openalex = dwr_sole_author,
    dwr_funding_other_openalex = dwr_funding_other
  )

crossref_subset <- crossref_metadata |>
  dplyr::mutate(
    publication_year_crossref = as.integer(publication_year),
    cited_by_count_crossref = as.integer(`is.referenced.by.count`)
  ) |>
  dplyr::select(
    doi,
    title_crossref = title,
    abstract_crossref = abstract,
    authors_crossref = authors,
    journal_crossref = `container.title`,
    publication_year_crossref,
    landing_page_url_crossref = url,
    cited_by_count_crossref,
    volume_crossref = volume,
    publisher_crossref = publisher,
    doi_source_crossref = doi_source,
    dwr_author_crossref = dwr_author,
    dwr_lead_author_crossref = dwr_lead_author,
    dwr_sole_author_crossref = dwr_sole_author,
    dwr_funding_other_crossref = dwr_funding_other
  )

all_metadata <- dplyr::full_join(openalex_subset, crossref_subset, by = "doi") |>
  dplyr::mutate(
    title = dplyr::coalesce(title_openalex, title_crossref),
    abstract = dplyr::coalesce(abstract_openalex, abstract_crossref),
    authors = purrr::map2(
      authors_openalex,
      authors_crossref,
      ~ {
        has_primary <- !is.null(.x) && length(.x) > 0 && !all(is.na(.x))
        if (has_primary) .x else .y
      }
    ),
    journal = dplyr::coalesce(journal_openalex, journal_crossref),
    publication_year = as.integer(dplyr::coalesce(publication_year_openalex, publication_year_crossref)),
    landing_page_url = dplyr::coalesce(landing_page_url_openalex, landing_page_url_crossref),
    cited_by_count = dplyr::coalesce(cited_by_count_openalex, cited_by_count_crossref),
    volume = dplyr::coalesce(volume_openalex, volume_crossref),
    publisher = dplyr::coalesce(publisher_crossref, publisher_openalex),
    doi_source = dplyr::coalesce(doi_source_openalex, doi_source_crossref),
    dwr_author = dplyr::coalesce(dwr_author_openalex, FALSE) | dplyr::coalesce(dwr_author_crossref, FALSE),
    dwr_lead_author = dplyr::coalesce(dwr_lead_author_openalex, FALSE) | dplyr::coalesce(dwr_lead_author_crossref, FALSE),
    dwr_sole_author = dplyr::coalesce(dwr_sole_author_openalex, FALSE) | dplyr::coalesce(dwr_sole_author_crossref, FALSE),
    dwr_funding_other = dplyr::coalesce(dwr_funding_other_openalex, FALSE) | dplyr::coalesce(dwr_funding_other_crossref, FALSE)
  ) |>
  dplyr::mutate(
    dwr_author = as.logical(dwr_author),
    dwr_lead_author = as.logical(dwr_lead_author),
    dwr_sole_author = as.logical(dwr_sole_author),
    dwr_funding_other = as.logical(dwr_funding_other)
  ) |>
  dplyr::select(
    doi,
    title,
    abstract,
    authors,
    canonical_affiliations,
    journal,
    publication_year,
    is_oa,
    is_oa_anywhere,
    oa_status,
    landing_page_url,
    pdf_url,
    license,
    cited_by_count,
    first_page,
    last_page,
    volume,
    issue,
    publisher,
    doi_source,
    author_affiliations,
    dwr_author,
    dwr_lead_author,
    dwr_sole_author,
    dwr_funding_other
  )

all_metadata <- all_metadata |>
  dplyr::mutate(
    dwr_funding_other = dplyr::if_else(
      doi_source == "manual" &
        !dplyr::coalesce(dwr_author, FALSE) &
        !dplyr::coalesce(dwr_lead_author, FALSE) &
        !dplyr::coalesce(dwr_sole_author, FALSE) &
        !dplyr::coalesce(dwr_funding_other, FALSE),
      TRUE,
      dwr_funding_other,
      missing = dwr_funding_other
    )
  )

# write out objects
usethis::use_data(
  dwr_doi_sources,
  crossref_metadata,
  openalex_metadata,
  all_metadata,
  affiliation_lookup,
  overwrite = TRUE
)
