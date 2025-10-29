# ---
# Purpose: Load the curated discipline taxonomy relevant to DWR research and
# store it as package data.
# ---

disciplines_taxonomy <- readr::read_csv(here::here("data-raw", "disciplines_taxonomy.csv")) |>
  dplyr::arrange(first_level)

usethis::use_data(disciplines_taxonomy, overwrite = TRUE)
