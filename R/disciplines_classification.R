#' Classify research articles by discipline via LLM.
#'
#' Provides a batching workflow that mirrors affiliation canonicalization while
#' constraining responses to the supplied taxonomy. Articles are described to
#' the model using their title and abstract (when available), and the
#' model returns a single second-level classification along with confidence and
#' a brief rationale.
#'
#' @param articles Data frame with `doi`, `title`, and `abstract`.
#' @param chat Optional pre-configured `ellmer` chat client.
#' @param model,base_url,api_key Connection details used when `chat` is `NULL`.
#' @param taxonomy Optional data frame overriding the packaged taxonomy.
#' @param batch_size Number of articles to submit per LLM batch.
#' @param max_active Maximum concurrent LLM requests.
#' @param rpm Requests-per-minute throttle for the LLM.
#' @param parallel_retries Retry attempts when parallel execution fails.
#' @param retry_backoff Seconds to wait between retries (scaled by attempt).
#' @param include_raw Should the raw LLM payload be returned?
#' @param verbose Emit progress updates and warnings?
#'
#' @return Tibble with `doi`, `second_level`, `first_level`, `confidence`,
#'   `note`, and optionally `raw_response`.
#'
#' @noRd
classify_disciplines <- function(
  articles,
  chat = NULL,
  model,
  base_url,
  api_key,
  taxonomy = NULL,
  batch_size = 10,
  max_active = 5,
  rpm = 60,
  parallel_retries = 2,
  retry_backoff = 2,
  include_raw = FALSE,
  verbose = interactive()
) {
  articles <- prepare_article_inputs(articles)

  taxonomy <- resolve_disciplines_taxonomy(taxonomy)
  taxonomy_prompt <- build_taxonomy_prompt(taxonomy)

  if (is.null(chat)) {
    if (missing(model) || missing(base_url) || missing(api_key)) {
      stop("`model`, `base_url`, and `api_key` are required unless `chat` is supplied.", call. = FALSE)
    }

    chat <- ellmer::chat_openai(
      model = model,
      base_url = base_url,
      api_key = api_key
    )
  }

  lookup <- articles
  lookup$id <- sprintf("article_%05d", seq_len(nrow(lookup)))
  lookup$second_level <- rep(NA_character_, nrow(lookup))
  lookup$confidence <- rep(NA_real_, nrow(lookup))
  lookup$note <- rep(NA_character_, nrow(lookup))
  if (include_raw) {
    lookup$raw_response <- rep(NA_character_, nrow(lookup))
  }

  lookup$prompt <- purrr::pmap_chr(
    list(lookup$title, lookup$abstract),
    function(title, abstract) build_article_prompt(title, abstract, taxonomy_prompt)
  )

  groups <- split_indices(nrow(lookup), batch_size)
  show_progress <- verbose && interactive()
  pb <- NULL

  if (show_progress) {
    pb <- utils::txtProgressBar(min = 0, max = nrow(lookup), style = 3)
    on.exit(close(pb), add = TRUE)
  }

  processed <- 0
  for (idxs in groups) {
    prompts <- lookup$prompt[idxs]
    ids <- lookup$id[idxs]

    results <- classify_parallel_prompts(
      prompts = prompts,
      ids = ids,
      chat = chat,
      taxonomy = taxonomy,
      max_active = max_active,
      rpm = rpm,
      parallel_retries = parallel_retries,
      retry_backoff = retry_backoff,
      include_raw = include_raw,
      verbose = verbose
    )

    if (length(results)) {
      for (res in results) {
        idx <- match(res$id, lookup$id)
        if (is.na(idx)) {
          next
        }

        lookup$second_level[idx] <- res$second_level
        lookup$confidence[idx] <- res$confidence
        lookup$note[idx] <- res$note
        if (include_raw) {
          lookup$raw_response[idx] <- res$raw_response
        }
      }
    }

    processed <- processed + length(idxs)
    if (!is.null(pb)) {
      utils::setTxtProgressBar(pb, processed)
    }
  }

  cols <- c("doi", "second_level", "confidence", "note")
  if (include_raw) {
    cols <- c(cols, "raw_response")
  }
  result <- lookup[, cols, drop = FALSE]
  result <- tibble::as_tibble(result)

  result <- dplyr::left_join(
    result,
    taxonomy[, c("second_level", "first_level")],
    by = "second_level"
  )

  cols <- c("doi", "second_level", "first_level", "confidence", "note")
  if (include_raw) {
    cols <- c(cols, "raw_response")
  }
  result <- result[, cols, drop = FALSE]
  result
}

#' Prepare and validate article inputs.
#'
#' Ensures required columns are present and coerces record fields to character.
#'
#' @param articles Data frame of article metadata.
#'
#' @return Tibble with normalized columns.
#'
#' @noRd
prepare_article_inputs <- function(articles) {
  if (is.null(articles)) {
    return(tibble::tibble(
      doi = character(),
      title = character(),
      abstract = character()
    ))
  }

  articles <- tibble::as_tibble(articles)
  required_cols <- c("doi", "title", "abstract")
  missing_cols <- setdiff(required_cols, names(articles))

  if (length(missing_cols)) {
    stop(
      "Articles must include columns: ",
      paste(required_cols, collapse = ", "),
      ". Missing: ",
      paste(missing_cols, collapse = ", "),
      call. = FALSE
    )
  }

  articles <- articles[, required_cols]
  articles$doi <- safe_char_vector(articles$doi, allow_missing = TRUE)
  articles$title <- safe_char_vector(articles$title)
  articles$abstract <- safe_char_vector(articles$abstract, allow_missing = TRUE)

  articles
}

#' Coerce vector elements to character safely.
#'
#' @param x Vector to coerce.
#' @param allow_missing Treat `NA` as empty character string?
#'
#' @return Character vector with `NA_character_` preserved when allowed.
#'
#' @noRd
safe_char_vector <- function(x, allow_missing = FALSE) {
  if (is.null(x)) {
    return(if (allow_missing) rep(NA_character_, 0) else character())
  }

  out <- as.character(x)
  if (!allow_missing) {
    out[is.na(out)] <- ""
  }
  out
}

#' Resolve the disciplines taxonomy to use.
#'
#' @param taxonomy Optional data frame with taxonomy columns.
#'
#' @return Tibble with `first_level`, `second_level`, and `description`.
#'
#' @noRd
resolve_disciplines_taxonomy <- function(taxonomy = NULL) {
  if (is.null(taxonomy)) {
    path <- system.file("disciplines_taxonomy.rda", package = "dwrpubs")
    if (!nzchar(path)) {
      path <- file.path("data", "disciplines_taxonomy.rda")
    }

    if (!file.exists(path)) {
      stop("Unable to locate `disciplines_taxonomy.rda`.", call. = FALSE)
    }

    env <- new.env(parent = emptyenv())
    loaded <- load(path, envir = env)
    if (!"disciplines_taxonomy" %in% loaded) {
      stop("`disciplines_taxonomy.rda` does not contain `disciplines_taxonomy`.", call. = FALSE)
    }

    taxonomy <- get("disciplines_taxonomy", envir = env)
  }

  taxonomy <- tibble::as_tibble(taxonomy)
  required_cols <- c("first_level", "second_level", "description")
  missing_cols <- setdiff(required_cols, names(taxonomy))

  if (length(missing_cols)) {
    stop(
      "`taxonomy` must include columns: ",
      paste(required_cols, collapse = ", "),
      ". Missing: ",
      paste(missing_cols, collapse = ", "),
      call. = FALSE
    )
  }

  taxonomy <- taxonomy[, required_cols]
  taxonomy$first_level <- safe_char_vector(taxonomy$first_level)
  taxonomy$second_level <- safe_char_vector(taxonomy$second_level)
  taxonomy$description <- safe_char_vector(taxonomy$description)
  taxonomy <- taxonomy[order(taxonomy$second_level), ]
  taxonomy
}

#' Build a taxonomy prompt fragment.
#'
#' Combines the taxonomy into a compact bullet list for prompt reuse.
#'
#' @param taxonomy Tibble returned by \code{resolve_disciplines_taxonomy()}.
#'
#' @return Character string describing allowed disciplines.
#'
#' @noRd
build_taxonomy_prompt <- function(taxonomy) {
  entries <- sprintf("- %s: %s", taxonomy$second_level, taxonomy$description)
  paste(
    "Only choose a discipline from the following list (second_level → description):",
    paste(entries, collapse = "\n"),
    sep = "\n"
  )
}

#' Build a prompt for a single article.
#'
#' @param title Article title.
#' @param abstract Article abstract (may be `NA`).
#' @param taxonomy_prompt Cached taxonomy instruction string.
#'
#' @return Character string prompt.
#'
#' @noRd
build_article_prompt <- function(title, abstract, taxonomy_prompt) {
  title <- if (isTRUE(nzchar(title))) title else "[Missing]"
  abstract_line <- if (!isTRUE(nzchar(abstract))) {
    "Abstract: [Missing]"
  } else {
    paste0("Abstract: ", abstract)
  }

  paste(
    "You are a classifier assigning each article to exactly one discipline.",
    "Follow the instructions carefully:",
    "1. Pick the single best `second_level` discipline from the allowed list.",
    "2. Never invent or modify discipline names.",
    "3. Always return JSON: {\"second_level\":\"...\",\"confidence\": <number 0-1>,\"note\":\"...\"}.",
    "4. Base your decision on the article's topic and substantive content, not on geographic cues (e.g., place names).",
    "5. Keep `note` brief—one short phrase explaining the match.",
    taxonomy_prompt,
    "Article metadata:",
    paste0("Title: ", title),
    abstract_line,
    "Respond with JSON only.",
    sep = "\n"
  )
}

#' Invoke the LLM for a batch of article prompts.
#'
#' Handles retries and sequential fallback mirroring the affiliation workflow.
#'
#' @inheritParams classify_disciplines
#' @param prompts Character vector of prompts.
#' @param ids Character vector of record identifiers matching `prompts`.
#'
#' @return List of result lists containing `id`, `second_level`, `confidence`,
#'   `note`, and optionally `raw_response`.
#'
#' @noRd
classify_parallel_prompts <- function(
  prompts,
  ids,
  chat,
  taxonomy,
  max_active,
  rpm,
  parallel_retries,
  retry_backoff,
  include_raw,
  verbose
) {
  if (!length(prompts)) {
    return(list())
  }

  prompt_payloads <- purrr::map(prompts, ellmer::ContentText)

  attempts <- parallel_retries + 1
  current_max_active <- max_active
  current_rpm <- rpm

  for (attempt in seq_len(attempts)) {
    response <- tryCatch(
      ellmer::parallel_chat_structured(
        chat = chat,
        prompts = prompt_payloads,
        type = classification_schema(),
        max_active = current_max_active,
        rpm = current_rpm,
        convert = FALSE
      ),
      error = function(e) e
    )

    if (!inherits(response, "error") && !is.null(response)) {
      parsed <- parse_classification_responses(
        response = response,
        ids = ids,
        taxonomy = taxonomy,
        include_raw = include_raw,
        verbose = verbose
      )

      if (length(parsed) == length(prompts)) {
        return(parsed)
      }

      if (verbose) {
        message(
          "Classification response length mismatch (received ",
          length(parsed),
          " vs expected ",
          length(prompts),
          ")."
        )
      }
    } else if (verbose) {
      failure_reason <- if (inherits(response, "error")) conditionMessage(response) else "LLM returned no response."
      message("Parallel classification failed: ", failure_reason)
    }

    if (attempt < attempts) {
      current_max_active <- max(1, floor(max_active / (attempt + 1)))
      current_rpm <- max(10, floor(rpm * (0.7 ^ attempt)))

      if (verbose) {
        message(
          "Retrying classification batch (attempt ",
          attempt + 1,
          " of ",
          attempts,
          ") with max_active=",
          current_max_active,
          ", rpm=",
          current_rpm,
          "."
        )
      }

      Sys.sleep(retry_backoff * attempt)
    }
  }

  if (verbose) {
    message("Classification exhausted retries; falling back to sequential prompts for current batch.")
  }

  purrr::map2(prompts, ids, function(prompt, id) {
    classify_single_prompt(
      prompt = prompt,
      id = id,
      chat = chat,
      taxonomy = taxonomy,
      include_raw = include_raw,
      verbose = verbose
    )
  })
}

#' Classification schema for structured responses.
#'
#' @return `ellmer` schema object matching the expected response payload.
#'
#' @noRd
classification_schema <- function() {
  ellmer::type_object(
    second_level = ellmer::type_string(),
    confidence = ellmer::type_number(),
    note = ellmer::type_string()
  )
}

#' Parse classification responses into structured records.
#'
#' @param response Raw response returned by the LLM.
#' @param ids Character vector of record identifiers.
#' @inheritParams classify_parallel_prompts
#'
#' @return List of parsed result lists.
#'
#' @noRd
parse_classification_responses <- function(response, ids, taxonomy, include_raw, verbose) {
  entries <- list()

  if (is.null(response)) {
    return(entries)
  }

  for (i in seq_along(ids)) {
    entry <- if (length(response) >= i) response[[i]] else NULL
    parsed <- coerce_classification_entry(
      entry = entry,
      id = ids[[i]],
      taxonomy = taxonomy,
      include_raw = include_raw,
      verbose = verbose
    )
    entries[[i]] <- parsed
  }

  entries
}

#' Coerce heterogenous LLM responses into a consistent record.
#'
#' @param entry Single response entry.
#' @param id Identifier corresponding to the article.
#' @inheritParams classify_parallel_prompts
#'
#' @return Parsed result list.
#'
#' @noRd
coerce_classification_entry <- function(entry, id, taxonomy, include_raw, verbose) {
  raw_text <- NA_character_
  if (is.character(entry) && length(entry)) {
    raw_text <- entry[[1]]
    parsed <- tryCatch(jsonlite::fromJSON(raw_text), error = function(e) NULL)
    entry <- parsed %||% entry
  }

  if (inherits(entry, "ellmer_structured")) {
    entry <- entry$to_list()
  }

  if (is.data.frame(entry)) {
    entry <- as.list(entry[1, ])
  }

  if (is.list(entry) && !is.null(entry$id) && is.null(entry$second_level) && is.null(entry$confidence)) {
    entry <- entry[[1]]
  }

  second_level <- safe_char(entry$second_level)
  confidence <- safe_num(entry$confidence)
  note <- safe_char(entry$note)

  if (!isTRUE(second_level %in% taxonomy$second_level)) {
    if (verbose && nzchar(second_level)) {
      message("Received unknown discipline '", second_level, "' for ", id, "; coercing to NA.")
    }
    second_level <- NA_character_
  }

  list(
    id = id,
    second_level = second_level,
    confidence = clamp_confidence(confidence),
    note = sanitize_note(note),
    raw_response = if (include_raw) raw_text else NULL
  )
}

#' Classify a single prompt sequentially.
#'
#' @inheritParams classify_parallel_prompts
#' @param prompt Prompt string.
#' @param id Record identifier.
#'
#' @return Parsed result list.
#'
#' @noRd
classify_single_prompt <- function(prompt, id, chat, taxonomy, include_raw, verbose) {
  result <- tryCatch(
    chat$chat_structured(
      ellmer::ContentText(prompt),
      type = classification_schema(),
      convert = FALSE
    ),
    error = function(e) e
  )

  if (inherits(result, "error")) {
    if (verbose) {
      message("Failed to classify article ", id, ": ", conditionMessage(result))
    }
    result <- NULL
  }

  coerce_classification_entry(
    entry = result,
    id = id,
    taxonomy = taxonomy,
    include_raw = include_raw,
    verbose = verbose
  )
}

#' Clamp a numeric confidence score to the unit interval.
#'
#' @param value Raw numeric value.
#'
#' @return Double between 0 and 1 or `NA_real_`.
#'
#' @noRd
clamp_confidence <- function(value) {
  if (is.na(value)) {
    return(NA_real_)
  }

  max(0, min(1, value))
}

#' Sanitise a short note from the LLM.
#'
#' @param note Raw character note.
#'
#' @return Character scalar or `NA_character_` when empty.
#'
#' @noRd
sanitize_note <- function(note) {
  note <- stringr::str_trim(note %||% "")
  note <- stringr::str_replace_all(note, "\\s+", " ")
  if (!nzchar(note)) {
    return(NA_character_)
  }
  note
}

#' Safely extract a numeric scalar.
#'
#' @param value Value possibly containing numeric data.
#'
#' @return Numeric scalar or `NA_real_`.
#'
#' @noRd
safe_num <- function(value) {
  if (is.null(value) || !length(value)) {
    return(NA_real_)
  }

  suppressWarnings(as.numeric(value[[1]]))
}
