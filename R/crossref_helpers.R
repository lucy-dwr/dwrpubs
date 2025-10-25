#' Crossref data helpers
#'
#' Helpers to build functional pipelines for retrieving Crossref works. The
#' functions follow a fetch → filter → extract structure so retrieval strategies
#' can be composed and reused.
NULL

#' Fetch Crossref works
#'
#' @param filters A named list of filters passed to [rcrossref::cr_works()]. Use
#'   Crossref filter keys such as ``"query.affiliation"`` or ``"funder"``.
#' @param limit Number of records to request. Defaults to 1000.
#' @param ... Additional arguments forwarded to [rcrossref::cr_works()].
#'
#' @return A tibble of Crossref works (possibly empty).
#'
#' @export
cr_fetch_works <- function(filters = list(), limit = 1000, ...) {
  res <- rcrossref::cr_works(filter = filters, limit = limit, ...)
  data <- res$data

  if (is.null(data) || !NROW(data)) {
    tibble::tibble()
  } else {
    tibble::as_tibble(data)
  }
}

#' Detect affiliation patterns within Crossref author records
#'
#' @param authors A data frame/list describing authors (typically the `author`
#'   column from `cr_fetch_works()` output).
#' @param pattern Pattern (regex or fixed string) passed to [base::grepl()].
#' @param fields Author-level fields inspected for the pattern.
#' @param ignore_case Should matching be case-insensitive? Defaults to ``TRUE``.
#'
#' @return Logical scalar stating whether the pattern was found.
#'
#' @export
cr_match_author_affiliation <- function(
  authors,
  pattern,
  fields = c("affiliation", "name", "family", "given"),
  ignore_case = TRUE
) {
  if (is.null(authors) || !length(authors)) {
    return(FALSE)
  }

  if (!is.data.frame(authors)) {
    authors <- tibble::as_tibble(authors)
  }

  has_pattern <- function(x) {
    any(stats::na.omit(
      grepl(pattern, x, ignore.case = ignore_case)
    ))
  }

  searchable <- intersect(fields, names(authors))

  if (length(searchable)) {
    for (field in searchable) {
      column <- authors[[field]]

      if (is.list(column)) {
        for (entry in column) {
          if (is.null(entry)) next

          if (is.data.frame(entry) && any(has_pattern(unlist(entry, use.names = FALSE)))) {
            return(TRUE)
          }

          if (is.atomic(entry) && has_pattern(entry)) {
            return(TRUE)
          }
        }
      } else if (has_pattern(column)) {
        return(TRUE)
      }
    }
  }

  FALSE
}

#' Filter Crossref works by affiliation pattern
#'
#' @param works Tibble from [cr_fetch_works()].
#' @param pattern Pattern to match within the `column`.
#' @param column Name of the column containing author information. Defaults to
#'   ``"author"``.
#' @param matcher Function used to detect matches. Defaults to
#'   [cr_match_author_affiliation()].
#' @param ... Additional arguments forwarded to the matcher.
#'
#' @return Filtered tibble.
#'
#' @export
cr_filter_affiliations <- function(
  works,
  pattern,
  column = "author",
  matcher = cr_match_author_affiliation,
  ...
) {
  if (!nrow(works)) {
    return(works)
  }

  if (!column %in% names(works)) {
    stop(sprintf("`works` must contain a `%s` column", column), call. = FALSE)
  }

  mask <- purrr::map_lgl(works[[column]], matcher, pattern = pattern, ...)
  dplyr::filter(works, mask)
}

#' Extract DOI values from Crossref works
#'
#' @inheritParams oa_extract_dois
#'
#' @return Character vector of DOI strings.
#'
#' @export
cr_extract_dois <- function(works, column = "doi", clean = TRUE, distinct = TRUE) {
  if (!nrow(works)) {
    return(character())
  }

  if (!column %in% names(works)) {
    warning(sprintf("`works` does not contain a `%s` column; returning empty vector.", column), call. = FALSE)
    return(character())
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

#' Look up Crossref funder identifiers
#'
#' @param query String describing the funder (e.g., organization name).
#' @param limit Maximum number of matches to return. Defaults to 5.
#'
#' @return A character vector of funder IDs (possibly empty).
#'
#' @export
cr_lookup_funder_ids <- function(query, limit = 5) {
  if (missing(query) || !nzchar(query)) {
    stop("`query` must be a non-empty string.", call. = FALSE)
  }

  result <- tryCatch(
    rcrossref::cr_funders(query = query, limit = limit),
    error = function(e) {
      warning("Unable to reach Crossref funder endpoint: ", conditionMessage(e), call. = FALSE)
      NULL
    }
  )

  if (is.null(result) || is.null(result$data) || !NROW(result$data)) {
    return(character())
  }

  result$data$id
}

#' Run Crossref retrieval strategies
#'
#' @param strategies Tibble describing strategy rows. Required columns:
#'   * `fetch` – list-column of arguments passed to [cr_fetch_works()].
#'   * `filter` – optional function applied to fetched data.
#'   * `post` – optional function applied after `filter`.
#' @param combine Function used to combine strategy outputs. Defaults to
#'   [dplyr::bind_rows()].
#'
#' @return Combined tibble.
#'
#' @export
cr_run_strategies <- function(strategies, combine = dplyr::bind_rows) {
  if (!all(c("fetch", "filter", "post") %in% names(strategies))) {
    stop("`strategies` must include `fetch`, `filter`, and `post` columns", call. = FALSE)
  }

  results <- purrr::map(seq_len(nrow(strategies)), function(i) {
    strategy <- strategies[i, ]
    fetch_args <- strategy$fetch[[1]] %||% list()
    works <- do.call(cr_fetch_works, fetch_args)

    filter_fn <- strategy$filter[[1]] %||% identity
    post_fn <- strategy$post[[1]] %||% identity

    post_fn(filter_fn(works))
  })

  do.call(combine, results)
}
