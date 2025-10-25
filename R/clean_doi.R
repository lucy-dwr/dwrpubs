#' Clean DOI strings
#'
#' Cleans DOI strings by trimming whitespace, removing wrapping quotes, and
#' extracting valid DOI substrings.
#'
#' @param x A character vector or list containing DOI-like strings.
#'
#' @return A character vector with cleaned DOIs. Missing or invalid entries are
#'   dropped, and the order of the remaining DOIs is preserved.
#'
#' @examples
#' clean_doi(c('"10.1234/abc"', "https://doi.org/10.5678/foo "))
#'
#' @importFrom stringr str_trim str_remove_all str_extract
#' @export
clean_doi <- function(x) {
  if (missing(x)) {
    stop("Argument `x` is required.", call. = FALSE)
  }

  x <- unlist(x, recursive = TRUE, use.names = FALSE)

  if (!length(x)) {
    return(character())
  }

  x <- stringr::str_trim(as.character(x))
  x <- stringr::str_remove_all(x, '^["\\\']|["\\\']$')

  dois <- stringr::str_extract(x, '\\b10\\.\\d{4,9}/[^\\s"\\)\\]\\,;]+')
  dois[!is.na(dois) & dois != ""]
}
