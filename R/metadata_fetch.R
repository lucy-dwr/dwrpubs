#' Fetch Crossref metadata for a set of DOIs
#'
#' @param dois Character vector of DOI strings.
#' @param chunk_size Number of DOIs to include per request batch.
#' @param pause Seconds to wait between requests, helping to respect rate
#'   limits.
#' @param progress Logical; show a text progress bar? Defaults to
#'   `interactive()`.
#'
#' @return Tibble of Crossref works (possibly empty).
#'
#' @export
fetch_crossref_metadata <- function(dois, chunk_size = 50, pause = 0.1, progress = interactive()) {
  if (!length(dois)) {
    return(tibble::tibble())
  }

  chunks <- chunk_vector(dois, chunk_size)
  n <- length(chunks)
  pb <- NULL

  if (progress) {
    pb <- utils::txtProgressBar(min = 0, max = n, style = 3)
    on.exit(close(pb), add = TRUE)
  }

  purrr::map2_dfr(chunks, seq_along(chunks), function(chunk, idx) {
    result <- tryCatch(
      rcrossref::cr_works(dois = chunk),
      error = function(e) {
        warning(
          "Crossref request failed for chunk starting with ",
          chunk[[1]],
          ": ",
          conditionMessage(e),
          call. = FALSE
        )
        NULL
      }
    )

    if (pause > 0) {
      Sys.sleep(pause)
    }

    if (!is.null(pb)) {
      utils::setTxtProgressBar(pb, idx)
    }

    if (is.null(result) || is.null(result$data) || !NROW(result$data)) {
      return(tibble::tibble())
    }

    tibble::as_tibble(result$data)
  })
}

#' Fetch OpenAlex metadata for a set of DOIs
#'
#' @param dois Character vector of DOI strings.
#' @param pause Seconds to wait between requests.
#' @param progress Logical; show a text progress bar? Defaults to
#'   `interactive()`.
#'
#' @return Tibble of OpenAlex works (possibly empty).
#'
#' @export
fetch_openalex_metadata <- function(dois, pause = 0.1, progress = interactive()) {
  if (!length(dois)) {
    return(tibble::tibble())
  }

  n <- length(dois)
  pb <- NULL

  if (progress) {
    pb <- utils::txtProgressBar(min = 0, max = n, style = 3)
    on.exit(close(pb), add = TRUE)
  }

  purrr::imap_dfr(dois, function(doi, idx) {
    identifier <- paste0("doi:", doi)

    result <- tryCatch(
      openalexR::oa_fetch(entity = "works", identifier = identifier),
      error = function(e) {
        warning(
          "OpenAlex request failed for DOI ",
          doi,
          ": ",
          conditionMessage(e),
          call. = FALSE
        )
        NULL
      }
    )

    if (pause > 0) {
      Sys.sleep(pause)
    }

    if (!is.null(pb)) {
      utils::setTxtProgressBar(pb, idx)
    }

    if (is.null(result) || !NROW(result)) {
      return(tibble::tibble())
    }

    result_tbl <- tibble::as_tibble(result)

    if ("abstract" %in% names(result_tbl)) {
      result_tbl$abstract <- strip_abstract_prefixes(result_tbl$abstract)
    }

    result_tbl
  })
}
