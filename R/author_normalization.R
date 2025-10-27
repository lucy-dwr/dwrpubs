#' Author normalization helpers
#'
#' Utility functions for working with Crossref and OpenAlex author data.
#'
#' @keywords internal
#' @name author_normalization_helpers
#' @noRd
NULL

#' Extract author display names from an OpenAlex authorships entry
#'
#' @param authorships Tibble/list from the `authorships` column of an OpenAlex
#'   work.
#' @param name_columns Candidate column names storing author display names.
#'
#' @return Character vector of unique, non-empty author names (possibly length
#'   zero).
#'
#' @export
extract_openalex_author_names <- function(
  authorships,
  name_columns = c("display_name", "author.display_name")
) {
  if (is.null(authorships) || !NROW(authorships)) {
    return(character())
  }

  if (!is.data.frame(authorships)) {
    authorships <- tibble::as_tibble(authorships)
  }

  cols <- intersect(name_columns, names(authorships))

  if (!length(cols)) {
    return(character())
  }

  names <- authorships[[cols[[1]]]]

  if (is.list(names)) {
    names <- unlist(names, use.names = FALSE)
  }

  names <- trimws(as.character(names))
  names <- names[nzchar(names) & !is.na(names)]

  unique(names)
}

#' Normalize Crossref author structures into nested tibbles
#'
#' Converts the heterogeneous author entries returned by Crossref into a tidy
#' two-column tibble (`author_name`, `author_affiliation`) for each work.
#'
#' @param authors List-column or vector where each element represents the author
#'   data for a Crossref work (as returned in `cr_fetch_works()`).
#'
#' @return A list of nested tibbles matching the length of `authors`.
#'
#' @export
format_crossref_authors <- function(authors) {
  purrr::map(authors, tidy_crossref_author_entry)
}

#' Normalize Crossref authors into a standard tibble
#'
#' Produces a tibble with `author_name` and `author_affiliation` columns,
#' regardless of whether the input is already normalized or still in the raw
#' Crossref structure.
#'
#' @inheritParams format_crossref_authors
#'
#' @return Tibble with one row per author.
#'
#' @export
normalize_crossref_authors <- function(authors) {
  if (is.null(authors) || !length(authors)) {
    return(tibble::tibble(
      author_name = character(),
      author_affiliation = character()
    ))
  }

  if (is.data.frame(authors) && "author_affiliation" %in% names(authors)) {
    affil <- authors$author_affiliation
    n <- nrow(authors)
    names <- if ("author_name" %in% names(authors)) authors$author_name else rep(NA_character_, n)

    return(tibble::tibble(
      author_name = names,
      author_affiliation = affil
    ))
  }

  tidy_crossref_author_entry(authors)
}

tidy_crossref_author_entry <- function(entry) {
  if (is.null(entry) || !length(entry)) {
    return(tibble::tibble(
      author_name = character(),
      author_affiliation = character()
    ))
  }

  if (!is.data.frame(entry)) {
    entry <- tibble::as_tibble(entry)
  }

  rows <- seq_len(nrow(entry))

  purrr::map_dfr(rows, function(i) {
    row <- entry[i, , drop = FALSE]

    name <- extract_crossref_author_name(row)
    affiliation_data <- if ("affiliation" %in% names(row)) row$affiliation[[1]] else NULL
    scalar_affils <- collect_scalar_affiliations(row)
    affiliation <- extract_crossref_affiliation(affiliation_data, scalar_affils)

    tibble::tibble(
      author_name = name,
      author_affiliation = affiliation
    )
  })
}

collect_scalar_affiliations <- function(row) {
  cols <- names(row)
  matches <- grepl("^affiliation", cols, ignore.case = TRUE)

  values <- unlist(row[cols[matches]], use.names = FALSE)
  values <- trimws(as.character(values))
  values <- values[nzchar(values)]

  if (!length(values)) {
    return(character())
  }

  values
}

extract_crossref_author_name <- function(row) {
  pick_value <- function(value) {
    if (is.null(value) || !length(value)) {
      return(NA_character_)
    }

    if (is.list(value)) {
      value <- unlist(value, use.names = FALSE)
    }

    value <- trimws(as.character(value))
    value <- value[nzchar(value)]

    if (!length(value)) {
      return(NA_character_)
    }

    value[[1]]
  }

  get_col <- function(x, col) {
    if (col %in% names(x)) x[[col]] else NULL
  }

  name <- pick_value(get_col(row, "name"))

  if (is.na(name)) {
    name <- pick_value(get_col(row, "literal"))
  }

  if (is.na(name)) {
    given <- pick_value(get_col(row, "given"))
    family <- pick_value(get_col(row, "family"))
    parts <- c(given, family)
    parts <- parts[!is.na(parts)]

    if (length(parts)) {
      name <- trimws(paste(parts, collapse = " "))
    }
  }

  if (is.na(name) || !nzchar(name)) {
    return(NA_character_)
  }

  name
}

extract_crossref_affiliation <- function(affiliation, extra = character()) {
  flatten <- function(x) {
    if (is.null(x) || !length(x)) {
      return(character())
    }

    if (is.data.frame(x)) {
      unlist(x, use.names = FALSE)
    } else if (is.list(x)) {
      unlist(x, recursive = TRUE, use.names = FALSE)
    } else {
      x
    }
  }

  values <- c(flatten(affiliation), extra)
  values <- trimws(as.character(values))
  values <- values[nzchar(values)]

  if (!length(values)) {
    return(NA_character_)
  }

  paste(unique(values)[1], collapse = "")
}
