#' Detect authorship matches on works
#'
#' Adds a logical column indicating whether the supplied pattern appears in the
#' authorship or author fields, depending on the backend source.
#'
#' @param data Tibble of works returned by `fetch_crossref_metadata()` or
#'   `fetch_openalex_metadata()`.
#' @param pattern Pattern (regex or fixed string) evaluated against affiliation
#'   and authorship fields.
#' @param source Data source structure; either ``"crossref"`` or ``"openalex"``.
#' @param column Optional override for the column holding authorship data. When
#'   omitted, defaults to ``"author"`` for Crossref and ``"authorships"`` for
#'   OpenAlex.
#' @param flag_column Name of the logical column to append.
#' @param authorship_type Character vector indicating which authorship roles to
#'   flag. Supported values are ``"any"`` (any matching author), ``"lead"``
#'   (first listed author matches), and ``"sole"`` (all available affiliations
#'   match). Defaults to ``"any"``. When multiple roles are requested the
#'   function appends one column per role.
#' @param ... Additional arguments forwarded to the underlying matcher.
#'
#' @return `data` with one or more added logical columns.
#'
#' @export
authorship_detection <- function(
  data,
  pattern,
  source = c("crossref", "openalex"),
  column = NULL,
  flag_column = "has_affiliation_match",
  authorship_type = "any",
  ...
) {
  stopifnot(is.data.frame(data))

  source <- match.arg(source)
  if (is.null(column)) {
    column <- if (source == "crossref") "author" else "authorships"
  }

  if (!column %in% names(data)) {
    stop(sprintf("`data` must contain a `%s` column.", column), call. = FALSE)
  }

  types <- match.arg(authorship_type, c("any", "lead", "sole"), several.ok = TRUE)
  types <- unique(types)
  matcher_args <- list(...)
  flag_names <- resolve_flag_column_names(flag_column, types)

  flagger <- switch(
    source,
    crossref = function(entry) detect_crossref_authorship(entry, pattern, types, matcher_args),
    openalex = function(entry) detect_openalex_authorship(entry, pattern, types, matcher_args)
  )

  flags <- purrr::map(data[[column]], flagger)

  for (type in types) {
    data[[flag_names[[type]]]] <- purrr::map_lgl(flags, `[[`, type)
  }

  data
}

resolve_flag_column_names <- function(flag_column, types) {
  if (!is.null(names(flag_column))) {
    missing <- setdiff(types, names(flag_column))
    if (length(missing)) {
      stop(sprintf("`flag_column` is missing names for: %s", paste(missing, collapse = ", ")), call. = FALSE)
    }

    return(flag_column[types])
  }

  if (length(flag_column) > 1) {
    stop("`flag_column` must be a single string or a named vector with entries for each `authorship_type`.", call. = FALSE)
  }

  stats::setNames(vapply(types, function(type) derive_flag_column(flag_column, type), character(1)), types)
}

derive_flag_column <- function(base, type) {
  if (type == "any") {
    return(base)
  }

  if (grepl("_author$", base)) {
    prefix <- sub("_author$", "", base)
    return(sprintf("%s_%s_author", prefix, type))
  }

  sprintf("%s_%s", base, type)
}

detect_crossref_authorship <- function(authors, pattern, types, matcher_args) {
  flags <- stats::setNames(rep(FALSE, length(types)), types)

  if (is.null(authors) || !length(authors)) {
    return(flags)
  }

  if ("any" %in% types) {
    flags[["any"]] <- do.call(
      cr_match_author_affiliation,
      c(list(authors = authors, pattern = pattern), matcher_args)
    )
  }

  needs_positional <- any(types %in% c("lead", "sole"))
  if (!needs_positional) {
    return(flags)
  }

  author_tbl <- normalize_crossref_authors(authors)
  if (!nrow(author_tbl)) {
    return(flags)
  }

  ignore_case <- matcher_args$ignore_case %||% TRUE
  affiliations <- stats::na.omit(author_tbl$author_affiliation)

  if ("lead" %in% types) {
    lead_affil <- author_tbl$author_affiliation[[1]]
    flags[["lead"]] <- !is.null(lead_affil) &&
      !is.na(lead_affil) &&
      grepl(pattern, lead_affil, ignore.case = ignore_case)
  }

  if ("sole" %in% types) {
    flags[["sole"]] <- length(affiliations) > 0 &&
      all(grepl(pattern, affiliations, ignore.case = ignore_case))
  }

  if ("any" %in% types) {
    positional <- intersect(c("lead", "sole"), names(flags))
    if (length(positional) && any(flags[positional])) {
      flags[["any"]] <- TRUE
    }
  }

  flags
}

detect_openalex_authorship <- function(authorships, pattern, types, matcher_args) {
  flags <- stats::setNames(rep(FALSE, length(types)), types)

  if (is.null(authorships) || !NROW(authorships)) {
    return(flags)
  }

  if ("any" %in% types) {
    flags[["any"]] <- do.call(
      oa_match_authorship_pattern,
      c(list(authorships = authorships, pattern = pattern), matcher_args)
    )
  }

  needs_positional <- any(types %in% c("lead", "sole"))
  if (!needs_positional) {
    return(flags)
  }

  ignore_case <- matcher_args$ignore_case %||% TRUE
  fields <- matcher_args$fields %||% c("display_name", "name", "affiliation_raw", "raw_affiliation")

  row_affiliations <- function(row) {
    collect_openalex_affiliations(row, fields)
  }

  if ("lead" %in% types) {
    lead_affils <- row_affiliations(authorships[1, , drop = FALSE])
    flags[["lead"]] <- length(lead_affils) > 0 &&
      any(grepl(pattern, lead_affils, ignore.case = ignore_case))
  }

  if ("sole" %in% types) {
    all_affils <- unlist(purrr::map(seq_len(nrow(authorships)), function(i) row_affiliations(authorships[i, , drop = FALSE])), use.names = FALSE)
    all_affils <- stats::na.omit(trimws(as.character(all_affils)))
    flags[["sole"]] <- length(all_affils) > 0 &&
      all(grepl(pattern, all_affils, ignore.case = ignore_case))
  }

  if ("any" %in% types) {
    positional <- intersect(c("lead", "sole"), names(flags))
    if (length(positional) && any(flags[positional])) {
      flags[["any"]] <- TRUE
    }
  }

  flags
}
