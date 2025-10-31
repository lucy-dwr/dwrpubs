#' Annotate metadata with author division information.
#'
#' Identifies the first author whose affiliation matches the supplied pattern
#' and assigns the corresponding employee division via fuzzy name matching.
#' Columns are appended using the provided prefix so callers can distinguish
#' organization-specific annotations.
#'
#' @param metadata Tibble produced in the metadata aggregation pipeline; must
#'   include `authors`, `author_affiliations`, and `publication_year` columns.
#' @param employees Tibble of employee division records as produced by
#'   `data-raw/employees.R`.
#' @param pattern Affiliation pattern (regex) used to identify org-linked
#'   authors.
#' @param author_flag_column Optional logical column in `metadata` restricting
#'   rows that should be considered for matching (e.g., `"dwr_author"`).
#' @param column_prefix Prefix applied to generated column names. The function
#'   appends `_author`, `_author_position`, and `_author_division`.
#' @param max_year_gap Maximum absolute difference between publication year and
#'   employee record year considered for fuzzy matching.
#' @param max_distance Maximum normalized Levenshtein distance allowed when
#'   matching author names to employee records. Distances above this threshold
#'   yield `NA` matches.
#'
#' @return A copy of `metadata` with three additional columns based on
#'   `column_prefix`.
#'
#' @keywords internal
annotate_author_divisions <- function(
  metadata,
  employees,
  pattern,
  author_flag_column = NULL,
  column_prefix = "first_affiliated",
  max_year_gap = 1L,
  max_distance = 0.3
) {
  stopifnot(is.data.frame(metadata), is.data.frame(employees))

  if (missing(pattern) || !nzchar(pattern)) {
    stop("`pattern` must be provided to locate affiliated authors.", call. = FALSE)
  }

  if (is.null(column_prefix) || !nzchar(column_prefix)) {
    stop("`column_prefix` must be a non-empty string.", call. = FALSE)
  }

  author_col <- paste0(column_prefix, "_author")
  position_col <- paste0(column_prefix, "_author_position")
  division_col <- paste0(column_prefix, "_author_division")

  if (!nrow(metadata)) {
    metadata[[author_col]] <- character()
    metadata[[position_col]] <- integer()
    metadata[[division_col]] <- character()
    return(metadata)
  }

  has_flag <- rep(TRUE, nrow(metadata))
  if (!is.null(author_flag_column)) {
    if (!author_flag_column %in% names(metadata)) {
      stop(sprintf("`metadata` must contain the `%s` column.", author_flag_column), call. = FALSE)
    }
    has_flag <- vapply(metadata[[author_flag_column]], isTRUE, logical(1))
  }

  first_results <- vector("list", nrow(metadata))

  for (i in seq_len(nrow(metadata))) {
    authors <- metadata$authors[[i]]
    affiliations <- metadata$author_affiliations[[i]]

    if (isTRUE(has_flag[[i]])) {
      first_results[[i]] <- find_first_affiliated_author(authors, affiliations, pattern)
    } else {
      first_results[[i]] <- list(
        name = NA_character_,
        position = NA_integer_
      )
    }
  }

  first_author_names <- vapply(first_results, function(x) x$name, character(1), USE.NAMES = FALSE)
  first_author_positions <- vapply(first_results, function(x) x$position, integer(1), USE.NAMES = FALSE)

  employee_index <- prepare_employee_index(employees)
  matches <- vector("list", nrow(metadata))

  for (i in seq_len(nrow(metadata))) {
    matches[[i]] <- match_author_to_employee(
      first_author_names[[i]],
      metadata$publication_year[[i]],
      employee_index,
      max_year_gap = max_year_gap,
      max_distance = max_distance
    )
  }

  metadata[[author_col]] <- first_author_names
  metadata[[position_col]] <- first_author_positions
  metadata[[division_col]] <- vapply(
    matches,
    function(x) x$division,
    character(1),
    USE.NAMES = FALSE
  )

  metadata
}

find_first_affiliated_author <- function(authors, affiliations, pattern) {
  result <- list(
    name = NA_character_,
    position = NA_integer_
  )

  if (is.null(authors) || !length(authors)) {
    return(result)
  }

  if (is.null(affiliations) || !is.data.frame(affiliations) || !nrow(affiliations)) {
    return(result)
  }

  if (!all(c("author", "canonical_affiliation") %in% names(affiliations))) {
    return(result)
  }

  author_names <- as.character(authors)
  affil_authors <- as.character(affiliations$author)
  affil_values <- as.character(affiliations$canonical_affiliation)

  for (i in seq_along(author_names)) {
    current_author <- author_names[[i]]
    if (is.na(current_author) || !nzchar(current_author)) {
      next
    }

    matching_rows <- !is.na(affil_authors) & affil_authors == current_author
    if (!any(matching_rows)) {
      next
    }

    values <- affil_values[matching_rows]
    values <- values[!is.na(values) & nzchar(values)]

    if (length(values) && any(grepl(pattern, values, ignore.case = TRUE))) {
      result$name <- current_author
      result$position <- i
      return(result)
    }
  }

  result
}

simplify_person_name <- function(name) {
  if (is.na(name) || !nzchar(name)) {
    return(NA_character_)
  }

  simple <- stringi::stri_trans_general(name, "Latin-ASCII")
  simple <- tolower(simple)
  simple <- gsub("[^a-z\\s]", " ", simple)
  simple <- gsub("\\s+", " ", trimws(simple))

  if (!nzchar(simple)) {
    return(NA_character_)
  }

  simple
}

extract_first_token <- function(name) {
  if (is.na(name) || !nzchar(name)) {
    return(NA_character_)
  }

  parts <- strsplit(name, " ", fixed = TRUE)[[1]]
  parts <- parts[nzchar(parts)]
  if (!length(parts)) {
    return(NA_character_)
  }

  parts[[1]]
}

extract_last_token <- function(name) {
  if (is.na(name) || !nzchar(name)) {
    return(NA_character_)
  }

  parts <- strsplit(name, " ", fixed = TRUE)[[1]]
  parts <- parts[nzchar(parts)]
  if (!length(parts)) {
    return(NA_character_)
  }

  parts[[length(parts)]]
}

prepare_employee_index <- function(employees) {
  required_cols <- c("employee_name", "division_adjusted", "year")
  missing_cols <- setdiff(required_cols, names(employees))

  if (length(missing_cols)) {
    stop(
      sprintf(
        "`employees` is missing required columns: %s",
        paste(missing_cols, collapse = ", ")
      ),
      call. = FALSE
    )
  }

  index <- employees
  index$year <- suppressWarnings(as.integer(index$year))

  index$name_clean <- vapply(
    index$employee_name,
    simplify_person_name,
    character(1),
    USE.NAMES = FALSE
  )

  alt_names <- vector("list", nrow(index))
  if ("employee_alternate_name" %in% names(index)) {
    alt_names <- lapply(index$employee_alternate_name, split_alternate_names)
  } else {
    alt_names <- replicate(nrow(index), character(), simplify = FALSE)
  }

  alt_clean <- lapply(alt_names, function(names) {
    if (!length(names)) {
      return(character())
    }
    cleaned <- vapply(
      names,
      simplify_person_name,
      character(1),
      USE.NAMES = FALSE
    )
    cleaned <- cleaned[!is.na(cleaned) & nzchar(cleaned)]
    unique(cleaned)
  })

  index$name_candidates <- Map(
    function(primary, alternates) {
      values <- c(primary, alternates)
      values <- values[!is.na(values) & nzchar(values)]
      if (!length(values)) {
        return(primary[!is.na(primary) & nzchar(primary)])
      }
      unique(values)
    },
    index$name_clean,
    alt_clean
  )

  index$last_name <- vapply(
    index$name_clean,
    extract_last_token,
    character(1),
    USE.NAMES = FALSE
  )

  index$last_name_candidates <- lapply(
    index$name_candidates,
    function(names) {
      if (!length(names)) {
        return(character())
      }
      vals <- vapply(
        names,
        extract_last_token,
        character(1),
        USE.NAMES = FALSE
      )
      vals <- vals[!is.na(vals) & nzchar(vals)]
      unique(vals)
    }
  )

  first_tokens <- vapply(
    index$name_clean,
    extract_first_token,
    character(1),
    USE.NAMES = FALSE
  )
  index$first_initial <- substr(first_tokens, 1, 1)

  index
}

match_author_to_employee <- function(
  author_name,
  publication_year,
  employee_index,
  max_year_gap,
  max_distance
) {
  default <- list(
    division = NA_character_,
    employee_name = NA_character_,
    distance = NA_real_,
    matched_year = NA_real_
  )

  if (is.null(author_name) || length(author_name) == 0L) {
    return(default)
  }

  if (is.na(author_name) || !nzchar(author_name)) {
    return(default)
  }

  author_clean <- simplify_person_name(author_name)
  if (is.na(author_clean) || !nzchar(author_clean)) {
    return(default)
  }

  author_last <- extract_last_token(author_clean)

  candidates <- employee_index

  if (!is.na(publication_year)) {
    year_diff <- abs(candidates$year - publication_year)
    within_gap <- !is.na(year_diff) & year_diff <= max_year_gap
    if (any(within_gap)) {
      candidates <- candidates[within_gap, , drop = FALSE]
    }
  }

  if (!is.na(author_last) && nzchar(author_last) && nrow(candidates)) {
    last_match <- vapply(
      candidates$last_name_candidates,
      function(vals) {
        if (is.null(vals) || !length(vals)) {
          return(FALSE)
        }
        any(vals == author_last)
      },
      logical(1)
    )
    if (any(last_match)) {
      candidates <- candidates[last_match, , drop = FALSE]
    }
  }

  if (!nrow(candidates)) {
    return(default)
  }

  distances <- vapply(
    seq_len(nrow(candidates)),
    function(i) {
      names <- candidates$name_candidates[[i]]
      min_normalized_distance(author_clean, names)
    },
    numeric(1)
  )

  finite_distances <- distances[is.finite(distances)]

  if (!length(finite_distances)) {
    return(default)
  }

  min_distance <- min(finite_distances)
  best_indices <- which(distances == min_distance)

  if (length(best_indices) > 1 && !is.na(publication_year)) {
    diffs <- abs(candidates$year[best_indices] - publication_year)
    diffs[is.na(diffs)] <- Inf
    best_indices <- best_indices[which.min(diffs)]
  } else {
    best_indices <- best_indices[[1]]
  }

  if (!is.na(max_distance) && is.finite(min_distance) && min_distance > max_distance) {
    return(default)
  }

  list(
    division = as.character(candidates$division_adjusted[best_indices]),
    employee_name = as.character(candidates$employee_name[best_indices]),
    distance = min_distance,
    matched_year = candidates$year[best_indices]
  )
}

compute_normalized_distance <- function(target, candidates) {
  if (!length(candidates)) {
    return(numeric())
  }

  candidates <- ifelse(is.na(candidates), "", candidates)
  target <- ifelse(is.na(target), "", target)

  raw <- utils::adist(target, candidates, ignore.case = TRUE)
  if (!is.matrix(raw)) {
    raw <- matrix(raw, nrow = 1)
  }

  raw_vals <- as.numeric(raw[1, , drop = TRUE])
  max_len <- pmax(nchar(target), nchar(candidates))
  if (!length(max_len)) {
    max_len <- rep(1, length(raw_vals))
  } else {
    max_len[max_len == 0] <- 1
  }

  raw_vals / max_len
}

min_normalized_distance <- function(target, candidates) {
  if (is.null(candidates) || !length(candidates)) {
    return(Inf)
  }

  distances <- compute_normalized_distance(target, candidates)
  if (!length(distances)) {
    return(Inf)
  }

  min(distances)
}

split_alternate_names <- function(value) {
  if (is.null(value) || is.na(value)) {
    return(character())
  }

  pieces <- unlist(strsplit(value, "\\s*[;|]\\s*"), use.names = FALSE)
  pieces <- trimws(pieces)
  pieces[nzchar(pieces)]
}
