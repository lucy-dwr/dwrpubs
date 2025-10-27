#' Build a prioritized DOI registry
#'
#' Given multiple DOI vectors with named origins, this helper cleans, deduplicates,
#' and stacks them in priority order so each DOI is tagged with the most reliable
#' source.
#'
#' @param ... Named DOI vectors supplied in descending reliability (e.g.,
#'   `manual`, `crossref`, `openalex`).
#'
#' @return A tibble with `doi` and `doi_source` columns.
#'
#' @export
build_doi_registry <- function(...) {
  dots <- rlang::list2(...)

  purrr::imap_dfr(dots, function(dois, source_name) {
    tibble::tibble(
      doi = clean_doi(dois),
      doi_source = source_name
    )
  }) |>
    dplyr::filter(!is.na(.data$doi), .data$doi != "") |>
    dplyr::distinct(.data$doi, .keep_all = TRUE)
}
