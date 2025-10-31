#' Author post-processing helpers
#'
#' Utilities for applying manual name replacements across the nested author
#' structures produced during metadata harmonisation.
#'
#' @name author_postprocessing
#' @keywords internal
NULL

#' Replace author names in list-columns
#'
#' Applies one or more name substitutions to a list-column of character vectors.
#'
#' @param authors_list List where each element contains the character vector of
#'   author names for a single work.
#' @param replacements Named character vector where the names correspond to the
#'   original values and the vector entries provide the replacements.
#'
#' @return Modified list matching the structure of `authors_list`.
#'
#' @examples
#' replace_author_names(
#'   list(c("Jane Doe", "John Smith")),
#'   c("John Smith" = "J. Smith")
#' )
#'
#' @keywords internal
#' @noRd
replace_author_names <- function(authors_list, replacements) {
  if (!is.list(authors_list) || !length(authors_list)) {
    return(authors_list)
  }

  if (missing(replacements) || !length(replacements)) {
    return(authors_list)
  }

  old_names <- names(replacements)
  if (is.null(old_names) || any(!nzchar(old_names))) {
    stop("`replacements` must be a named character vector.", call. = FALSE)
  }

  purrr::map(authors_list, function(authors) {
    if (is.null(authors)) {
      return(authors)
    }

    for (i in seq_along(replacements)) {
      mask <- authors == old_names[[i]]
      if (any(mask, na.rm = TRUE)) {
        authors[mask] <- replacements[[i]]
      }
    }

    authors
  })
}

#' Replace author names in affiliation tibbles
#'
#' Applies the supplied name substitutions to the `author` column within each
#' tibble of canonical author-affiliation pairs.
#'
#' @inheritParams replace_author_names
#'
#' @return Modified list-column mirroring `affiliations_list`.
#'
#' @examples
#' replace_author_affiliation_names(
#'   list(tibble::tibble(author = "Jane Doe", canonical_affiliation = NA_character_)),
#'   c("Jane Doe" = "J. Doe")
#' )
#'
#' @keywords internal
#' @noRd
replace_author_affiliation_names <- function(affiliations_list, replacements) {
  if (!is.list(affiliations_list) || !length(affiliations_list)) {
    return(affiliations_list)
  }

  if (missing(replacements) || !length(replacements)) {
    return(affiliations_list)
  }

  old_names <- names(replacements)
  if (is.null(old_names) || any(!nzchar(old_names))) {
    stop("`replacements` must be a named character vector.", call. = FALSE)
  }

  purrr::map(affiliations_list, function(tbl) {
    if (is.null(tbl) || !is.data.frame(tbl) || !"author" %in% names(tbl)) {
      return(tbl)
    }

    for (i in seq_along(replacements)) {
      mask <- tbl$author == old_names[[i]]
      if (any(mask, na.rm = TRUE)) {
        tbl$author[mask] <- replacements[[i]]
      }
    }

    tbl
  })
}

#' Remove blocked canonical affiliations from affiliation tibbles
#'
#' Filters out rows whose canonical affiliation appears in a supplied blocklist.
#'
#' @param affiliations_list List-column of author affiliation tibbles.
#' @param blocked Character vector of canonical affiliations to discard.
#' @param column Name of the column to inspect (defaults to `canonical_affiliation`).
#'
#' @return Modified list-column where blocked affiliations have been removed.
#'
#' @keywords internal
#' @noRd
remove_blocked_affiliations <- function(affiliations_list, blocked, column = "canonical_affiliation") {
  if (!is.list(affiliations_list) || !length(affiliations_list)) {
    return(affiliations_list)
  }

  if (missing(blocked) || !length(blocked)) {
    return(affiliations_list)
  }

  purrr::map(affiliations_list, function(tbl) {
    if (is.null(tbl) || !is.data.frame(tbl) || !nrow(tbl) || !column %in% names(tbl)) {
      return(tbl)
    }

    mask <- !is.na(tbl[[column]]) & tbl[[column]] %in% blocked
    keep <- !mask
    tbl[keep, , drop = FALSE]
  })
}

#' Remove blocked canonical affiliation values
#'
#' Filters character vectors so that any values present in the blocklist are removed.
#'
#' @param affiliations_list List-column of character vectors.
#' @param blocked Character vector of canonical affiliations to discard.
#'
#' @return Modified list-column with blocked values removed.
#'
#' @keywords internal
#' @noRd
remove_blocked_affiliation_values <- function(affiliations_list, blocked) {
  if (!is.list(affiliations_list) || !length(affiliations_list)) {
    return(affiliations_list)
  }

  if (missing(blocked) || !length(blocked)) {
    return(affiliations_list)
  }

  purrr::map(affiliations_list, function(values) {
    if (is.null(values)) {
      return(values)
    }

    mask <- !is.na(values) & values %in% blocked
    values <- values[!mask]
    values
  })
}

#' Override first affiliated author details
#'
#' Updates the first affiliated author columns (`*_author`, `*_author_position`,
#' and `*_author_division`) for selected DOIs. The division value is sourced from
#' the employees roster using the supplied author name and author position.
#'
#' @param metadata Metadata tibble returned by `annotate_author_divisions()`.
#' @param employees Employee tibble containing `employee_name`, `division_adjusted`,
#'   and `year`.
#' @param overrides A data frame or tibble with at least `doi`, `author`,
#'   `position`, and `column_prefix` columns. Optional `publication_year` can be
#'   supplied to refine the employee lookup; when omitted the value from
#'   `metadata$publication_year` is used.
#' @param max_year_gap Maximum difference between publication year and employee
#'   record year when locating a division.
#' @param max_distance Maximum normalized Levenshtein distance allowed when
#'   matching author names to employee records.
#'
#' @return Updated `metadata` tibble with overrides applied.
#'
#' @keywords internal
#' @noRd
override_first_affiliated_author <- function(
  metadata,
  employees,
  overrides,
  max_year_gap = 1L,
  max_distance = 0.3
) {
  if (!nrow(metadata) || !nrow(overrides)) {
    return(metadata)
  }

  required_override_cols <- c("doi", "author", "position", "column_prefix")
  missing_cols <- setdiff(required_override_cols, names(overrides))
  if (length(missing_cols)) {
    stop(
      sprintf("`overrides` is missing required columns: %s", paste(missing_cols, collapse = ", ")),
      call. = FALSE
    )
  }

  employee_index <- prepare_employee_index(employees)
  override_indices <- match(overrides$doi, metadata$doi)
  valid <- !is.na(override_indices)
  if (!any(valid)) {
    return(metadata)
  }

  override_indices <- override_indices[valid]
  overrides <- overrides[valid, , drop = FALSE]

  for (i in seq_len(nrow(overrides))) {
    row <- overrides[i, , drop = FALSE]
    idx <- override_indices[[i]]

    prefix <- row$column_prefix[[1]]
    author_col <- paste0(prefix, "_author")
    position_col <- paste0(prefix, "_author_position")
    division_col <- paste0(prefix, "_author_division")

    if (!all(c(author_col, position_col, division_col) %in% names(metadata))) {
      next
    }

    metadata[[author_col]][[idx]] <- row$author[[1]]
    metadata[[position_col]][[idx]] <- as.integer(row$position[[1]])

    pub_year <- if ("publication_year" %in% names(row) && !is.na(row$publication_year[[1]])) {
      row$publication_year[[1]]
    } else {
      metadata$publication_year[[idx]]
    }

    match <- match_author_to_employee(
      author_name = row$author[[1]],
      publication_year = pub_year,
      employee_index = employee_index,
      max_year_gap = max_year_gap,
      max_distance = max_distance
    )

    metadata[[division_col]][[idx]] <- match$division
  }

  metadata
}

#' Override DWR classification flags
#'
#' Manually sets one or more `dwr_*` boolean columns for specified DOIs.
#'
#' @param metadata Metadata tibble containing the `dwr_*` columns.
#' @param overrides Tibble/data frame with columns `doi`, `columns`, and `value`.
#'   `columns` can be a character vector per row specifying which `dwr_*`
#'   columns to update. `value` must be coercible to logical.
#'
#' @return Updated `metadata` tibble.
#'
#' @keywords internal
#' @noRd
override_dwr_flags <- function(metadata, overrides) {
  if (!nrow(metadata) || !nrow(overrides)) {
    return(metadata)
  }

  required_cols <- c("doi", "columns", "value")
  missing_cols <- setdiff(required_cols, names(overrides))
  if (length(missing_cols)) {
    stop(
      sprintf("`overrides` is missing required columns: %s", paste(missing_cols, collapse = ", ")),
      call. = FALSE
    )
  }

  for (i in seq_len(nrow(overrides))) {
    row <- overrides[i, , drop = FALSE]
    idx <- which(metadata$doi == row$doi[[1]])
    if (!length(idx)) {
      next
    }

    cols <- row$columns[[1]]
    if (is.character(cols)) {
      cols <- list(cols)
    }
    cols <- unlist(cols, use.names = FALSE)
    cols <- cols[cols %in% names(metadata)]
    if (!length(cols)) {
      next
    }

    value <- as.logical(row$value[[1]])
    metadata[idx, cols] <- lapply(metadata[idx, cols, drop = FALSE], function(x) value)
  }

  metadata
}
