# ---
# Purpose: Classify articles into disciplines using a taxonomy and an LLM.
# ---

load(here::here("data", "all_metadata.rda"))
load(here::here("data", "disciplines_taxonomy.rda"))

new_classified_inventory <- TRUE

if (new_classified_inventory) {
  to_classify <- all_metadata |>
    dplyr::select(doi, title, abstract)

  classified_articles <- classify_disciplines(
    to_classify,
    model = "gemini-2.5-flash",
    base_url = "https://generativelanguage.googleapis.com/v1beta/",
    api_key = Sys.getenv("GOOGLE_API_KEY"),
    batch_size = 10,
    max_active = 5,
    rpm = 60,
    parallel_retries = 2,
    retry_backoff = classification_llm$retry_backoff,
    include_raw = FALSE,
    verbose = interactive()
  )
} else {
  load(here::here("data", "classified_inventory.rda"))
  classified_articles <- classified_inventory |>
    dplyr::select(doi, second_level, first_level, confidence, note)
}

classified_inventory <- all_metadata |>
  dplyr::left_join(classified_articles, by = "doi") |>
  dplyr::relocate(first_level, second_level, confidence, note, .after = abstract)

usethis::use_data(classified_inventory, overwrite = TRUE)
