# ---
# Purpose: Clean manually curated DWR DOIs before storing them as package data.
# ---

load(here::here("data", "dois_drop.rda"))

dois_manual <- readr::read_lines(here::here("data-raw", "dois_manual.csv"))
dois_manual <- clean_doi(dois_manual)
dois_manual <- unique(dois_manual)
dois_manual <- dois_manual[!dois_manual %in% dois_drop]

usethis::use_data(dois_manual, overwrite = TRUE)
