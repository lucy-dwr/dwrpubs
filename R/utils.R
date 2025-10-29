#' Null-coalescing operator
#'
#' Returns the left-hand side unless it is `NULL`, in which case the
#' right-hand side is returned. Useful for providing lightweight defaults when
#' working with list-columns.
#'
#' @param x Primary value to evaluate.
#' @param y Fallback value used when `x` is `NULL`.
#'
#' @return Either `x` or `y`.
#'
#' @keywords internal
#' @export
`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}

#' Split a vector into equally sized chunks
#'
#' Convenience wrapper used to batch large vectors in API requests.
#'
#' @param x Vector to split.
#' @param size Maximum chunk size.
#'
#' @return A list of vector chunks (possibly empty).
#'
#' @export
chunk_vector <- function(x, size) {
  if (!length(x)) {
    return(list())
  }

  split(x, ceiling(seq_along(x) / size))
}

#' Remove leading abstract labels
#'
#' Strips common heading fragments (e.g., "Background" or "ABSTRACT") that are
#' sometimes glued to the start of abstract fields without a separating space.
#'
#' @param text Character vector of abstract strings.
#'
#' @return Character vector with prefixes removed where applicable.
#'
#' @keywords internal
strip_abstract_prefixes <- function(text) {
  if (!length(text)) {
    return(text)
  }

  text <- as.character(text)
  keep <- !is.na(text) & nzchar(text)

  if (!any(keep)) {
    return(text)
  }

  working <- trimws(text[keep])
  working <- sub("^['\"]+", "", working)

  authors_idx <- grepl("^(?i)Author(?:\\(s\\)|s)?\\s*:", working, perl = TRUE)
  corresponding_idx <- grepl("^(?i)Corresponding\\s+author\\s*:", working, perl = TRUE)
  corresponding_any_idx <- grepl("(?i)corresponding\\s+author", working, perl = TRUE)
  copyright_idx <- grepl(
    "^(?:\\x{00A9}|\\(c\\)|&copy;|(?i)copyright)",
    working,
    perl = TRUE
  )
  wrapped_brackets_idx <- grepl("^\\[.*\\]$", working)
  ams_idx <- grepl("American\\s+Meteorological\\s+Society", working, ignore.case = TRUE)
  goodridge_idx <- grepl("Jim\\s+Goodridge", working, ignore.case = TRUE)
  drop_idx <- authors_idx |
    corresponding_idx |
    corresponding_any_idx |
    copyright_idx |
    wrapped_brackets_idx |
    ams_idx |
    goodridge_idx

  if (any(drop_idx)) {
    working[drop_idx] <- NA_character_
  }

  remaining <- !drop_idx & !is.na(working) & nzchar(working)

  if (any(remaining)) {
    working_remaining <- working[remaining]

    strip_before_tokens <- c("20192", "12180-8349", "95819", "33559")
    for (token in strip_before_tokens) {
      token_idx <- grepl(token, working_remaining, fixed = TRUE)
      idx <- which(!is.na(token_idx) & token_idx)
      if (length(idx)) {
        pattern <- paste0("^.*?", token)
        working_remaining[idx] <- sub(
          pattern,
          "",
          working_remaining[idx],
          perl = TRUE
        )
      }
    }

    sfews_idx <- grepl("SFEWS\\s+Editors", working_remaining, ignore.case = TRUE)
    sfews_idx <- which(!is.na(sfews_idx) & sfews_idx)
    if (length(sfews_idx)) {
      working_remaining[sfews_idx] <- NA_character_
    }

    working_remaining <- sub("^(?i)Background\\s+", "", working_remaining, perl = TRUE)
    working_remaining <- sub("^(?i)A\\s*bstract\\s+", "", working_remaining, perl = TRUE)
    working_remaining <- sub("^(?i)Objective\\s+", "", working_remaining, perl = TRUE)
    working_remaining <- sub("^(?i)Introduction\\s+", "", working_remaining, perl = TRUE)
    working_remaining <- sub("^ABSTRACT\\s*", "", working_remaining, perl = TRUE)
    working_remaining <- sub("^(?i)Abstract\\.?\\s+", "", working_remaining, perl = TRUE)

    sticky_labels <- c(
      "Background",
      "Key Takeaways",
      "Context",
      "Objective",
      "Motivation",
      "Motivations"
    )

    for (label in sticky_labels) {
      pattern <- paste0("^", label, "(?! )[:;,-]*")
      working_remaining <- sub(pattern, "", working_remaining, perl = TRUE)

      pattern_space <- paste0("^", label, "\\s+")
      working_remaining <- sub(pattern_space, "", working_remaining, perl = TRUE)
    }

    working_remaining <- trimws(working_remaining)
    working[remaining] <- working_remaining
  }

  text[keep] <- working
  text
}

#' Clean article titles
#'
#' Removes inline formatting tags and converts shout-cased titles to sentence
#' case so downstream consumers work with consistent title strings.
#'
#' @param data Data frame (or tibble) containing a `title` column.
#' @return The input data with a cleaned `title` column.
#'
#' @export
clean_article_titles <- function(data) {
  if (is.null(data) || !NROW(data)) {
    return(data)
  }

  if (!"title" %in% names(data)) {
    warning("`data` does not contain a `title` column; returning input unchanged.", call. = FALSE)
    return(data)
  }

  titles <- data$title
  titles_chr <- vapply(
    seq_along(titles),
    function(i) {
      entry <- titles[[i]]

      if (is.null(entry) || !length(entry)) {
        return(NA_character_)
      }

      while (is.list(entry) && length(entry)) {
        entry <- entry[[1]]
      }

      if (is.null(entry) || !length(entry)) {
        return(NA_character_)
      }

      value <- entry[[1]]

      if (is.null(value) || !length(value) || (is.character(value) && is.na(value))) {
        return(NA_character_)
      }

      val <- as.character(value)
      if (!nzchar(val)) {
        return(val)
      }

      val
    },
    character(1),
    USE.NAMES = FALSE
  )

  if (!length(titles_chr)) {
    data$title <- titles_chr
    return(data)
  }

  replacements <- titles_chr
  keep <- !is.na(replacements)

  if (any(keep)) {
    replacements[keep] <- gsub("<\\/?i>", "", replacements[keep], ignore.case = TRUE, perl = TRUE)
    replacements[keep] <- gsub("<\\/?scp>", "", replacements[keep], ignore.case = TRUE, perl = TRUE)
    replacements[keep] <- gsub("<\\/?sub>", "", replacements[keep], ignore.case = TRUE, perl = TRUE)

    all_caps_idx <- keep &
      replacements == toupper(replacements) &
      grepl("[A-Z]", replacements, perl = TRUE)

    if (any(all_caps_idx, na.rm = TRUE)) {
      replacements[all_caps_idx] <- stringr::str_to_sentence(
        stringr::str_to_lower(replacements[all_caps_idx])
      )
    }
  }

  data$title <- replacements
  data
}
