#' Flag works whose funding matches a pattern
#'
#' @inheritParams flag_authorship
#' @param flag_column Name of the logical column added to the data; defaults to
#'   ``"has_funding_match"``.
#'
#' @return `data` with a logical funding indicator.
#'
#' @export
flag_funding <- function(
  data,
  pattern,
  source = c("crossref", "openalex"),
  column = NULL,
  flag_column = "has_funding_match",
  ...
) {
  stopifnot(is.data.frame(data))
  source <- match.arg(source)

  if (is.null(column)) {
    column <- if (source == "crossref") "funder" else "grants"
  }

  if (!column %in% names(data)) {
    stop(sprintf("`data` must contain a `%s` column.", column), call. = FALSE)
  }

  matcher <- switch(
    source,
    crossref = match_crossref_funder,
    openalex = match_openalex_grant
  )

  flags <- purrr::map_lgl(data[[column]], matcher, pattern = pattern, ...)
  data[[flag_column]] <- flags
  data
}

match_crossref_funder <- function(funders, pattern, fields = c("name", "award"), ignore_case = TRUE) {
  if (is.null(funders) || !length(funders)) {
    return(FALSE)
  }

  if (!is.data.frame(funders)) {
    funders <- tibble::as_tibble(funders)
  }

  cols <- intersect(fields, names(funders))
  if (!length(cols)) {
    return(FALSE)
  }

  any(grepl(pattern, unlist(funders[cols], use.names = FALSE), ignore.case = ignore_case))
}

match_openalex_grant <- function(grants, pattern, ignore_case = TRUE) {
  if (is.null(grants) || !length(grants)) {
    return(FALSE)
  }

  if (!is.data.frame(grants)) {
    grants <- tibble::as_tibble(grants)
  }

  flat <- unlist(grants, use.names = FALSE)
  flat <- stats::na.omit(flat)

  if (!length(flat)) {
    return(FALSE)
  }

  any(grepl(pattern, flat, ignore.case = ignore_case))
}
