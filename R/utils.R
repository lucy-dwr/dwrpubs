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
