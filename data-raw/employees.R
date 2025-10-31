# ---
# Purpose: Import a list of DWR employees associated with divisions
# and years of employement.
# ---

# Note that this file isn't hosted on GitHub due to privacy concerns -
# it's currently on Lucy's laptop. It will need to be hosted somewhere
# more permanent and imported from that source in a future update.

# Columns in this file:
# - employee_name: full name of the employee
# - employee_alternate_name: alternate name/spelling for the employee
# - division_adjusted: division within DWR where the employee worked in
#     the given year
# - year: year of employment record
# - division_old: old division name for divisions whose identity has changed

employees <- readr::read_csv(
  here::here("data-raw", "dwr_employee_division_year.csv"),
  col_types = "cccc"
) |>
  dplyr::mutate(
    employee_name = stringr::str_to_title(employee_name),
    employee_alternate_name = stringr::str_to_title(employee_alternate_name)
  ) |>
  unique()

usethis::use_data(employees, overwrite = TRUE)

