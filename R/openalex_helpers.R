#' OpenAlex data helpers
#'
#' A small collection of helpers to build functional pipelines for retrieving
#' and filtering OpenAlex works. The helpers are designed to be composable so
#' you can mix & match fetch, filter, and extraction steps when new retrieval
#' strategies are needed.
NULL

#' Fetch OpenAlex works with readable defaults
#'
#' @param ... Named arguments forwarded to [openalexR::oa_fetch()]. The
#'   `entity` argument defaults to ``"works"`` but can be overridden.
#' @param entity The OpenAlex entity to fetch. Defaults to ``"works"``.
#'
#' @return A tibble returned by `openalexR::oa_fetch()`.
#'
#' @export
oa_fetch_works <- function(..., entity = "works") {
  openalexR::oa_fetch(entity = entity, ...)
}

#' Test whether an authorship entry mentions a pattern
#'
#' @param authorships A tibble/list returned in the `authorships` column of
#'   OpenAlex works.
#' @param pattern Pattern (regex or fixed string) used with [base::grepl()].
#' @param fields Candidate column names to search within nested affiliation
#'   tables.
#' @param ignore_case Should matches ignore case? Defaults to ``TRUE``.
#'
#' @return Logical scalar indicating whether the pattern was found.
#'
#' @export
oa_match_authorship_pattern <- function(
  authorships,
  pattern,
  fields = c("display_name", "name", "affiliation_raw", "raw_affiliation"),
  ignore_case = TRUE
) {
  if (is.null(authorships) || !NROW(authorships)) {
    return(FALSE)
  }

  has_pattern <- function(x) {
    any(stats::na.omit(
      grepl(pattern, x, ignore.case = ignore_case)
    ))
  }

  if ("affiliation_raw" %in% names(authorships) && has_pattern(authorships$affiliation_raw)) {
    return(TRUE)
  }

  if (!"affiliations" %in% names(authorships)) {
    return(FALSE)
  }

  for (aff in authorships$affiliations) {
    if (is.null(aff)) {
      next
    }

    if (is.data.frame(aff) && NROW(aff)) {
      cols <- intersect(fields, names(aff))

      if (length(cols) && has_pattern(unlist(aff[cols], use.names = FALSE))) {
        return(TRUE)
      }

      if (!length(cols) && has_pattern(apply(aff, 1, paste, collapse = " "))) {
        return(TRUE)
      }
    } else if (is.atomic(aff) && has_pattern(aff)) {
      return(TRUE)
    }
  }

  FALSE
}

#' Filter works by authorship pattern
#'
#' @param works Tibble of works (e.g. result of [oa_fetch_works()]).
#' @param pattern Pattern to look for in the `authorships` column.
#' @param matcher Custom function taking `authorships` and returning logical; by
#'   default [oa_match_authorship_pattern()] is used.
#' @param ... Additional arguments passed to the matcher.
#'
#' @return A filtered tibble of works.
#'
#' @export
oa_filter_authorships <- function(works, pattern, matcher = oa_match_authorship_pattern, ...) {
  if (!"authorships" %in% names(works)) {
    stop("`works` must contain an `authorships` column", call. = FALSE)
  }

  mask <- purrr::map_lgl(works$authorships, matcher, pattern = pattern, ...)
  dplyr::filter(works, mask)
}

#' Extract DOI values from OpenAlex works
#'
#' @param works Tibble of works containing a DOI column.
#' @param column Name of the DOI column; defaults to ``"doi"``.
#' @param clean Should [clean_doi()] be applied? Defaults to ``TRUE``.
#' @param distinct Should duplicates be removed? Defaults to ``TRUE``.
#'
#' @return Character vector of DOI strings.
#'
#' @export
oa_extract_dois <- function(works, column = "doi", clean = TRUE, distinct = TRUE) {
  if (!column %in% names(works)) {
    stop(sprintf("`works` must contain a `%s` column", column), call. = FALSE)
  }

  dois <- works[[column]]

  if (clean) {
    dois <- clean_doi(dois)
  }

  if (distinct) {
    dois <- unique(dois)
  }

  stats::na.omit(dois)
}

#' Look up OpenAlex funder identifiers
#'
#' @param query Text used to search the funder catalog (e.g., organization
#'   names or keywords).
#' @param per_page Number of matches to request from the API. Defaults to 5.
#'
#' @return Character vector of funder IDs (possibly empty).
#'
#' @export
oa_lookup_funder_ids <- function(query, per_page = 5) {
  if (missing(query) || !nzchar(query)) {
    stop("`query` must be a non-empty string.", call. = FALSE)
  }

  result <- tryCatch(
    openalexR::oa_fetch(entity = "funders", search = query, per_page = per_page),
    error = function(e) {
      warning("Unable to reach OpenAlex funder endpoint: ", conditionMessage(e), call. = FALSE)
      NULL
    }
  )

  if (is.null(result) || !NROW(result)) {
    return(character())
  }

  result$id
}

#' Run a set of OpenAlex retrieval strategies
#'
#' @param strategies A tibble/data.frame where each row contains the arguments
#'   required to run one retrieval strategy. Expected columns are:
#'   * `fetch` – list-columns of arguments passed to [oa_fetch_works()].
#'   * `filter` – optional function applied to the fetched works.
#'   * `post` – optional function applied after `filter`.
#' @param combine Function used to merge all results. Defaults to
#'   [dplyr::bind_rows()].
#'
#' @return A tibble combining the outputs of the supplied strategies.
#'
#' @export
oa_run_strategies <- function(strategies, combine = dplyr::bind_rows) {
  if (!all(c("fetch", "filter", "post") %in% names(strategies))) {
    stop("`strategies` must include `fetch`, `filter`, and `post` columns", call. = FALSE)
  }

  results <- purrr::map(seq_len(nrow(strategies)), function(i) {
    strategy <- strategies[i, ]
    fetch_args <- strategy$fetch[[1]] %||% list()
    works <- do.call(oa_fetch_works, fetch_args)

    filter_fn <- strategy$filter[[1]] %||% identity
    post_fn <- strategy$post[[1]] %||% identity

    post_fn(filter_fn(works))
  })

  do.call(combine, results)
}
