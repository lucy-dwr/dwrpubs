#' @keywords internal
#' @noRd
collapse_unique <- function(values) {
  if (is.null(values)) {
    return(NA_character_)
  }

  values <- trimws(as.character(values))
  values <- values[nzchar(values) & !is.na(values)]
  values <- unique(values)

  if (!length(values)) {
    return(NA_character_)
  }

  paste(values, collapse = "; ")
}

#' @keywords internal
#' @noRd
derive_author_string <- function(authors) {
  collapse_unique(authors)
}

#' @keywords internal
#' @noRd
derive_affiliation_string <- function(affiliations_tbl) {
  if (is.null(affiliations_tbl) || !is.data.frame(affiliations_tbl) || !nrow(affiliations_tbl)) {
    return(NA_character_)
  }

  collapse_unique(affiliations_tbl$canonical_affiliation)
}

#' @keywords internal
#' @noRd
format_harvard_citation <- function(authors, publication_year, title, journal, volume, issue, first_page, last_page, doi) {
  components <- c(
    format_authors(authors),
    format_year(publication_year),
    format_title(title),
    format_journal(journal),
    format_volume_issue(volume, issue),
    collapse_pages(first_page, last_page),
    format_doi(doi)
  )

  components <- components[nzchar(components)]
  paste(components, collapse = " ")
}

#' @keywords internal
#' @noRd
summarise_dwr_contribution <- function(sole, lead, any, funded) {
  authored <- dplyr::case_when(
    isTRUE(sole) ~ "sole authored",
    isTRUE(lead) ~ "lead authored",
    isTRUE(any) ~ "co-authored",
    TRUE ~ ""
  )

  funded_text <- if (isTRUE(funded)) "funded" else ""
  parts <- c(authored, funded_text)
  parts <- parts[nzchar(parts)]

  if (!length(parts)) "" else paste(parts, collapse = "; ")
}

#' @keywords internal
#' @noRd
format_authors <- function(authors) {
  author_string <- format_author_list(authors)
  if (!nzchar(author_string)) {
    return("")
  }

  if (grepl("\\.$", author_string)) {
    return(author_string)
  }

  paste0(author_string, ".")
}

#' @keywords internal
#' @noRd
format_author_list <- function(authors) {
  if (is.null(authors) || !length(authors)) {
    return("")
  }

  authors <- trimws(as.character(authors))
  authors <- authors[nzchar(authors) & !is.na(authors)]
  authors <- unique(authors)

  if (!length(authors)) {
    return("")
  }

  use_et_al <- length(authors) > 8
  if (use_et_al) {
    authors <- authors[seq_len(8)]
  }

  formatted <- purrr::map_chr(authors, format_single_author)
  formatted <- formatted[nzchar(formatted)]

  if (!length(formatted)) {
    return("")
  }

  if (use_et_al) {
    author_block <- collapse_author_block(formatted)
    return(paste(author_block, "et al"))
  }

  if (length(formatted) == 1) {
    return(append_final_period(formatted))
  }

  if (length(formatted) == 2) {
    return(paste(
      append_final_period(formatted[1]),
      append_final_period(formatted[2]),
      sep = " and "
    ))
  }

  paste(
    paste(purrr::map_chr(formatted[-length(formatted)], append_final_period), collapse = ", "),
    append_final_period(formatted[length(formatted)]),
    sep = ", and "
  )
}

#' @keywords internal
#' @noRd
format_single_author <- function(name) {
  parts <- strsplit(name, "\\s+")[[1]]
  parts <- parts[nzchar(parts)]

  if (!length(parts)) {
    return("")
  }

  surname <- parts[length(parts)]
  initials_parts <- parts[-length(parts)]

  if (!length(initials_parts)) {
    return(surname)
  }

  initials <- purrr::map_chr(initials_parts, function(part) {
    cleaned <- gsub("[^A-Za-z]", "", part)
    substr(cleaned, 1, 1)
  })
  initials <- initials[nzchar(initials)]

  if (!length(initials)) {
    return(surname)
  }

  initials <- paste0(initials, ".", collapse = "")
  paste0(surname, ", ", initials)
}

#' @keywords internal
#' @noRd
collapse_author_block <- function(formatted_authors) {
  n <- length(formatted_authors)

  if (n <= 1) {
    return(paste(purrr::map_chr(formatted_authors, append_final_period), collapse = ", "))
  }

  if (n == 2) {
    return(paste(
      append_final_period(formatted_authors[1]),
      append_final_period(formatted_authors[2]),
      sep = " and "
    ))
  }

  paste(
    paste(purrr::map_chr(formatted_authors[-n], append_final_period), collapse = ", "),
    append_final_period(formatted_authors[n]),
    sep = ", and "
  )
}

#' @keywords internal
#' @noRd
append_final_period <- function(author_string) {
  if (!nzchar(author_string)) {
    return(author_string)
  }

  if (grepl("\\.$", author_string)) {
    return(author_string)
  }

  paste0(author_string, ".")
}

#' @keywords internal
#' @noRd
format_year <- function(year) {
  if (is.na(year)) {
    return("")
  }

  paste0("(", year, ").")
}

#' @keywords internal
#' @noRd
format_title <- function(title) {
  if (is.na(title) || !nzchar(title)) {
    return("")
  }

  paste0(title, ".")
}

#' @keywords internal
#' @noRd
format_journal <- function(journal) {
  if (is.na(journal) || !nzchar(journal)) {
    return("")
  }

  paste0(journal, ".")
}

#' @keywords internal
#' @noRd
format_volume_issue <- function(volume, issue) {
  if (is.na(volume) || !nzchar(volume)) {
    return("")
  }

  if (is.na(issue) || !nzchar(issue)) {
    return(paste0(volume, "."))
  }

  paste0(volume, "(", issue, ").")
}

#' @keywords internal
#' @noRd
collapse_pages <- function(first_page, last_page) {
  if (is.na(first_page) || !nzchar(first_page)) {
    return("")
  }

  if (is.na(last_page) || !nzchar(last_page) || identical(first_page, last_page)) {
    return(paste0(first_page, "."))
  }

  paste0(first_page, "-", last_page, ".")
}

#' @keywords internal
#' @noRd
format_doi <- function(doi) {
  if (is.na(doi) || !nzchar(doi)) {
    return("")
  }

  paste0("doi:", doi, ".")
}

#' @keywords internal
#' @noRd
sanitize_text <- function(values) {
  if (is.null(values)) {
    return(values)
  }

  out <- stringi::stri_trans_general(values, "Latin-ASCII")
  out[is.na(values)] <- NA_character_
  out
}
