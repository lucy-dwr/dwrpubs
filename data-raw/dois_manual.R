dois_manual <- readr::read_lines(here::here("data-raw", "dois_manual.csv"))
dois_manual <- clean_doi(dois_manual)

usethis::use_data(dois_manual, overwrite = TRUE)