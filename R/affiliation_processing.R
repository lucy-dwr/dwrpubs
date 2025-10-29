#' Build a chat client for affiliation canonicalization.
#'
#' Constructs an `ellmer::chat_openai()` client with the supplied connection
#' details. The helper centralises validation so that all canonicalization
#' entry points share the same guard rails.
#'
#' @param model The model identifier to pass to the LLM backend.
#' @param base_url Base URL for the LLM service.
#' @param api_key API key or token used to authenticate with the service.
#'
#' @return An `ellmer` chat client ready for structured chats.
#'
#' @noRd
default_affiliation_chat <- function(model, base_url, api_key) {
  if (missing(model) || !nzchar(model)) {
    stop("`model` must be supplied to canonicalize affiliations.", call. = FALSE)
  }

  if (missing(base_url) || !nzchar(base_url)) {
    stop("`base_url` must be supplied to canonicalize affiliations.", call. = FALSE)
  }

  if (missing(api_key) || !nzchar(api_key)) {
    stop("`api_key` must be supplied to canonicalize affiliations.", call. = FALSE)
  }

  ellmer::chat_openai(
    model = model,
    base_url = base_url,
    api_key = api_key
  )
}

#' Canonicalize a vector of affiliations.
#'
#' Given a character vector (or list) of affiliation strings, this helper
#' orchestrates batching, LLM calls, post-processing, and final normalization.
#' When `chat` is `NULL` it instantiates a default chat client with the supplied
#' credentials. The result is a tibble mapping each original string to its
#' canonical counterpart.
#'
#' @param values Character vector or list of affiliation strings.
#' @param chat Optional pre-configured `ellmer` chat client.
#' @param model,base_url,api_key Connection details used when `chat` is `NULL`.
#' @param batch_size Number of groups to canonicalize per LLM batch.
#' @param group_size Number of affiliations per prompt group.
#' @param max_active Maximum concurrent LLM requests.
#' @param rpm Requests per minute throttle for the LLM.
#' @param parallel_retries Number of retries when parallel canonicalization fails.
#' @param retry_backoff Multiplicative backoff applied to `rpm` and `max_active`.
#' @param verbose Should progress updates and warnings be emitted?
#'
#' @return A tibble with `original` and normalized `canonical` columns.
#'
#' @noRd
canonicalize_affiliation_values <- function(
  values,
  chat = NULL,
  model,
  base_url,
  api_key,
  batch_size = 25,
  group_size = 10,
  max_active = 5,
  rpm = 200,
  parallel_retries = 2,
  retry_backoff = 2,
  verbose = interactive()
) {
  values <- flatten_affiliation_values(values)

  if (!length(values)) {
    return(tibble::tibble(original = character(), canonical = character()))
  }

  lookup <- tibble::tibble(
    id = sprintf("affil_%05d", seq_along(values)),
    original = values,
    canonical = NA_character_
  )

  if (is.null(chat)) {
    if (missing(model) || missing(base_url) || missing(api_key)) {
      stop("`model`, `base_url`, and `api_key` are required unless `chat` is supplied.", call. = FALSE)
    }

    chat <- default_affiliation_chat(
      model = model,
      base_url = base_url,
      api_key = api_key
    )
  }

  show_progress <- verbose && interactive()
  pb <- NULL
  total_items <- nrow(lookup)
  if (show_progress) {
    pb <- utils::txtProgressBar(min = 0, max = total_items, style = 3)
    on.exit(close(pb), add = TRUE)
  }

  groups <- split_indices(total_items, group_size)
  batches <- split_indices(length(groups), batch_size)
  processed <- 0

  for (batch in batches) {
    group_batch <- groups[batch]
    prompts <- purrr::map(group_batch, function(idxs) build_group_prompt(lookup[idxs, , drop = FALSE]))
    ids_list <- purrr::map(group_batch, function(idxs) lookup$id[idxs])
    originals_list <- purrr::map(group_batch, function(idxs) lookup$original[idxs])

    results <- canonicalize_group_prompts(
      prompts = prompts,
      ids_list = ids_list,
      originals_list = originals_list,
      chat = chat,
      max_active = max_active,
      rpm = rpm,
      parallel_retries = parallel_retries,
      retry_backoff = retry_backoff,
      verbose = verbose
    )

    lookup <- apply_group_results(lookup, ids_list, results)

    processed <- processed + sum(purrr::map_int(group_batch, length))
    if (!is.null(pb)) {
      utils::setTxtProgressBar(pb, min(processed, total_items))
    }
  }

  lookup$canonical_raw <- lookup$canonical
  lookup$canonical <- finalize_canonical_values(lookup$canonical_raw)

  lookup[, c("original", "canonical")]
}

#' Canonicalize a single affiliation string with the LLM.
#'
#' Wraps prompt construction, chat invocation, and normalization for a single
#' affiliation. If the LLM cannot provide a value, a deterministic fallback is
#' derived from the raw input.
#'
#' @param value Raw affiliation string.
#' @param chat Chat client returned by \code{default_affiliation_chat()}.
#' @param verbose Should failures be messaged to the console?
#'
#' @return A single canonicalized affiliation string.
#'
#' @noRd
canonicalize_single_affiliation <- function(value, chat, verbose = FALSE) {
  prompt <- build_affiliation_prompt(value)
  result <- tryCatch(
    chat$chat_structured(
      ellmer::ContentText(prompt),
      type = affiliation_schema()
    ),
    error = function(e) e
  )

  institution <- ""
  if (!inherits(result, "error")) {
    institution <- extract_canonical_value(result)
  } else if (verbose) {
    message("Failed to canonicalize affiliation via LLM: ", conditionMessage(result))
  }

  institution <- clean_canonical_value(institution)
  if (!isTRUE(nzchar(institution))) {
    institution <- fallback_affiliation_name(value)
  }

  institution
}

#' Canonicalize a batch of affiliation prompt groups.
#'
#' Coordinates LLM calls for grouped prompts, handling retries, throttling,
#' and fallbacks when responses are incomplete.
#'
#' @param prompts List of prompt strings to submit.
#' @param ids_list List of affiliation id vectors matching each prompt.
#' @param originals_list List of original affiliation vectors per prompt.
#' @param chat Chat client used for the structured calls.
#' @param max_active Maximum number of concurrent LLM requests.
#' @param rpm Requests-per-minute throttle for the LLM.
#' @param parallel_retries Number of retry attempts when parallel execution fails.
#' @param retry_backoff Backoff multiplier applied between retries.
#' @param verbose Should diagnostic messages be emitted?
#'
#' @return List of named character vectors keyed by affiliation id.
#'
#' @noRd
canonicalize_group_prompts <- function(
  prompts,
  ids_list,
  originals_list,
  chat,
  max_active,
  rpm,
  parallel_retries,
  retry_backoff,
  verbose
) {
  if (!length(prompts)) {
    return(list())
  }

  attempts <- parallel_retries + 1
  current_max_active <- max_active
  current_rpm <- rpm

  for (attempt in seq_len(attempts)) {
    response <- tryCatch(
      ellmer::parallel_chat_structured(
        chat = chat,
        prompts = prompts,
        type = affiliation_array_schema(),
        max_active = current_max_active,
        rpm = current_rpm,
        convert = FALSE
      ),
      error = function(e) e
    )

    valid_response <- !inherits(response, "error") && !is.null(response)

    if (valid_response) {
      parsed <- parse_group_responses(response)

      if (length(parsed) == length(prompts)) {
        return(purrr::map2(parsed, seq_along(parsed), function(group_result, idx) {
          resolve_group_results(
            ids = ids_list[[idx]],
            originals = originals_list[[idx]],
            parsed = group_result,
            chat = chat,
            verbose = verbose
          )
        }))
      }

      if (verbose) {
        message(
          "Parallel response length mismatch (received ",
          length(parsed),
          " vs expected ",
          length(prompts),
          ")."
        )
      }
    } else if (verbose) {
      failure_reason <- if (inherits(response, "error")) conditionMessage(response) else "LLM returned no response."
      message("Parallel canonicalization failed: ", failure_reason)
    }

    if (attempt < attempts) {
      current_max_active <- max(1, floor(max_active / (attempt + 1)))
      current_rpm <- max(10, floor(rpm * (0.7 ^ attempt)))

      if (verbose) {
        message(
          "Retrying prompt batch (attempt ",
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
    message("Parallel canonicalization exhausted retries; falling back to sequential prompts for current batch.")
  }

  sequential_group_fallback(
    prompts = prompts,
    ids_list = ids_list,
    originals_list = originals_list,
    chat = chat,
    verbose = verbose
  )
}

#' Build the prompt used for single-affiliation canonicalization.
#'
#' The resulting prompt asks the LLM to return a single canonical institution
#' name encoded as JSON.
#'
#' @param value Affiliation string to canonicalize.
#'
#' @return Character string prompt.
#'
#' @noRd
build_affiliation_prompt <- function(value) {
  paste(
    "You normalize institution names extracted from affiliation strings.",
    "Rules:",
    "1. Return only the canonical institution name—omit departments, colleges, centers, labs, and geographic details when a parent institution exists.",
    "2. Spell out university names in full (e.g., write 'University of California Berkeley', not 'UC Berkeley').",
    "3. Expand abbreviations for United States government entities (e.g., 'U.S.' → 'United States' and 'EPA' → 'Environmental Protection Agency').",
    "4. Prefer the parent institution when a college/school belongs to it (e.g., 'State University of New York College of Environmental Science and Forestry' → 'State University of New York').",
    "5. Only keep 'College', 'School', etc. when it is the actual institution name (e.g., 'Colby College').",
    "Respond ONLY with JSON like {\"canonical_institution\": \"<institution>\"}.",
    "Affiliation:", value,
    "Canonical institution:",
    sep = "\n"
  )
}

#' Build a prompt describing a group of affiliations.
#'
#' @param entries Tibble with `id` and `original` columns for a prompt group.
#'
#' @return Character string prompt listing all affiliations to canonicalize.
#'
#' @noRd
build_group_prompt <- function(entries) {
  lines <- c(
    "You normalize institution names extracted from affiliation strings.",
    "Rules:",
    "1. Return only canonical institution names—omit departments, colleges, centers, labs, and geographic details when a parent institution exists.",
    "2. Spell out university names fully (e.g., 'University of California Berkeley', not 'UC Berkeley').",
    "3. Expand abbreviations for United States government entities (e.g., 'U.S.' → 'United States' and 'EPA' → 'Environmental Protection Agency').",
    "4. Prefer the parent institution when a college/school belongs to it.",
    "5. Only keep 'College', 'School', etc. when it is the actual institution name.",
    "Respond ONLY with JSON like [{\"id\": \"<id>\", \"canonical_institution\": \"<institution>\"}, ...].",
    "Canonicalize the following affiliations:"
  )

  affil_lines <- sprintf("%s: %s", entries$id, entries$original)
  paste(c(lines, affil_lines), collapse = "\n")
}

#' Structured schema for single-affiliation responses.
#'
#' @return An `ellmer` schema object with a `canonical_institution` field.
#'
#' @noRd
affiliation_schema <- function() {
  ellmer::type_object(
    canonical_institution = ellmer::type_string()
  )
}

#' Structured schema for grouped affiliation responses.
#'
#' @return An `ellmer` array schema of affiliation objects.
#'
#' @noRd
affiliation_array_schema <- function() {
  ellmer::type_array(
    ellmer::type_object(
      id = ellmer::type_string(),
      canonical_institution = ellmer::type_string()
    )
  )
}

#' Parse structured responses returned by the LLM.
#'
#' Normalises list, tibble, and JSON representations into a list of tibbles
#' with consistent column names.
#'
#' @param response Raw response returned by the chat client.
#'
#' @return List of tibbles with `id` and `canonical_institution` columns.
#'
#' @noRd
parse_group_responses <- function(response) {
  if (is.null(response)) {
    return(list())
  }

  if (is.list(response) && !is.data.frame(response)) {
    return(purrr::map(response, coerce_group_entries))
  }

  if (is.data.frame(response)) {
    return(list(coerce_group_entries(response)))
  }

  list()
}

#' Coerce heterogeneous LLM entries into a standard tibble.
#'
#' @param entry Single response entry (list, tibble, or JSON string).
#'
#' @return Tibble with `id` and `canonical_institution` columns.
#'
#' @noRd
coerce_group_entries <- function(entry) {
  if (is.null(entry)) {
    return(tibble::tibble(id = character(), canonical_institution = character()))
  }

  if (is.data.frame(entry)) {
    cols <- names(entry)
    id_col <- cols[which(cols %in% c("id", "identifier"))][1]
    canon_col <- cols[which(cols %in% c("canonical_institution", "canonical", "institution"))][1]

    if (!is.na(id_col) && !is.na(canon_col)) {
      return(tibble::tibble(
        id = as.character(entry[[id_col]]),
        canonical_institution = as.character(entry[[canon_col]])
      ))
    }
  }

  if (is.list(entry)) {
    if (length(entry) && all(purrr::map_lgl(entry, is.list))) {
      ids <- purrr::map_chr(entry, ~ safe_char(.x$id))
      canon <- purrr::map_chr(entry, ~ safe_char(.x$canonical_institution))
      return(tibble::tibble(id = ids, canonical_institution = canon))
    }

    if (!is.null(entry$id) && !is.null(entry$canonical_institution)) {
      return(tibble::tibble(
        id = safe_char(entry$id),
        canonical_institution = safe_char(entry$canonical_institution)
      ))
    }
  }

  if (is.character(entry) && length(entry)) {
    parsed <- tryCatch(jsonlite::fromJSON(entry), error = function(e) NULL)
    if (!is.null(parsed)) {
      return(coerce_group_entries(parsed))
    }
  }

  tibble::tibble(id = character(), canonical_institution = character())
}

#' Coerce a possibly missing value to character safely.
#'
#' @param x Value extracted from an LLM response.
#'
#' @return Character scalar or `NA_character_`.
#'
#' @noRd
safe_char <- function(x) {
  if (is.null(x) || !length(x)) {
    return(NA_character_)
  }

  as.character(x[[1]])
}

#' Resolve canonical results for a group of affiliations.
#'
#' Performs fallback canonicalization when the LLM omits an entry or returns an
#' empty value, keeping the output aligned with the input ids.
#'
#' @param ids Character vector of affiliation ids.
#' @param originals Raw affiliation strings corresponding to `ids`.
#' @param parsed Parsed response tibble obtained from \code{parse_group_responses()}.
#' @param chat Chat client used for single-affiliation fallbacks.
#' @param verbose Should informative messages be emitted?
#'
#' @return Named character vector of canonical affiliations.
#'
#' @noRd
resolve_group_results <- function(ids, originals, parsed, chat, verbose) {
  parsed <- coerce_group_entries(parsed)
  resolved <- stats::setNames(rep(NA_character_, length(ids)), ids)

  if (nrow(parsed)) {
    matches <- match(ids, parsed$id)
    found <- !is.na(matches)
    resolved[found] <- parsed$canonical_institution[matches[found]]
  }

  resolved <- purrr::imap_chr(resolved, function(value, id) {
    idx <- match(id, ids)
    cleaned <- clean_canonical_value(value %||% "")
    if (!isTRUE(nzchar(cleaned))) {
      canonicalize_single_affiliation(originals[idx], chat, verbose)
    } else {
      cleaned
    }
  })

  resolved
}

#' Apply resolved group results onto the lookup table.
#'
#' @param lookup Tibble with `id`, `original`, and `canonical` columns.
#' @param ids_list List of id vectors used when building prompts.
#' @param results_list List of canonical vectors keyed by id.
#'
#' @return Updated lookup tibble.
#'
#' @noRd
apply_group_results <- function(lookup, ids_list, results_list) {
  if (!length(results_list)) {
    return(lookup)
  }

  for (i in seq_along(results_list)) {
    result <- results_list[[i]]
    if (is.null(result) || !length(result)) {
      next
    }

    idx <- match(names(result), lookup$id)
    valid <- !is.na(idx)
    lookup$canonical[idx[valid]] <- result[valid]
  }

  lookup
}

#' Canonicalize each prompt sequentially when parallel execution fails.
#'
#' @param prompts Character vector of group prompts.
#' @param ids_list List of id vectors matching `prompts`.
#' @param originals_list List of original affiliation vectors.
#' @param chat Chat client for structured calls.
#' @param verbose Should diagnostic messages be shown?
#'
#' @return List of canonical vectors keyed by affiliation id.
#'
#' @noRd
sequential_group_fallback <- function(prompts, ids_list, originals_list, chat, verbose) {
  purrr::map(seq_along(prompts), function(i) {
    canonicalize_group_prompt(
      prompt = prompts[[i]],
      ids = ids_list[[i]],
      originals = originals_list[[i]],
      chat = chat,
      verbose = verbose
    )
  })
}

#' Canonicalize a single prompt describing multiple affiliations.
#'
#' @inheritParams canonicalize_group_prompts
#' @param prompt Prompt string describing the group.
#'
#' @return Named character vector of canonical affiliations.
#'
#' @noRd
canonicalize_group_prompt <- function(prompt, ids, originals, chat, verbose) {
  result <- tryCatch(
    chat$chat_structured(
      ellmer::ContentText(prompt),
      type = affiliation_array_schema(),
      convert = FALSE
    ),
    error = function(e) e
  )

  parsed <- if (!inherits(result, "error")) result else NULL

  if (inherits(result, "error") && verbose) {
    message("Failed to canonicalize affiliations via group prompt: ", conditionMessage(result))
  }

  resolve_group_results(ids, originals, parsed, chat, verbose)
}

#' Derive a deterministic fallback affiliation name.
#'
#' Removes geographic qualifiers and sub-units to produce a stable canonical
#' guess when the LLM cannot provide one.
#'
#' @param value Raw affiliation string.
#'
#' @return Simplified affiliation string.
#'
#' @noRd
fallback_affiliation_name <- function(value) {
  fallback <- trimws(as.character(value))
  fallback <- stringr::str_replace_all(fallback, "\\s+", " ")
  fallback <- stringr::str_remove(fallback, ",?\\s*(United States( of America)?|USA|U\\.S\\.A\\.?|U\\.S\\.)\\s*$")
  fallback <- stringr::str_remove(fallback, ",?\\s*(CA|California|OR|Oregon|WA|Washington|NV|Nevada|AZ|Arizona|TX|Texas)\\s*\\d{3,5}.*$")
  fallback <- sub(",.*$", "", fallback)
  fallback <- trimws(fallback)

  if (!nzchar(fallback)) {
    fallback <- trimws(as.character(value))
  }

  fallback
}

#' Clean the canonical value returned by the LLM.
#'
#' Trims whitespace, collapses internal spacing, and converts placeholder
#' strings such as `"[NA]"` into proper `NA_character_` values.
#'
#' @param value Value returned by the LLM.
#'
#' @return A cleaned character scalar or `NA_character_`.
#'
#' @noRd
clean_canonical_value <- function(value) {
  if (is.null(value) || !length(value)) {
    return("")
  }

  out <- trimws(as.character(value[1]))
  out <- stringr::str_replace_all(out, "\\s+", " ")
  if (identical(out, "[NA]")) {
    return(NA_character_)
  }
  out
}

#' Flatten affiliation values into a unique character vector.
#'
#' Ensures nested lists are collapsed, whitespace is normalised, and missing
#' values removed before canonicalization runs.
#'
#' @param values Character vector or list of affiliations.
#'
#' @return Deduplicated character vector of affiliations.
#'
#' @noRd
flatten_affiliation_values <- function(values) {
  if (is.null(values)) {
    return(character())
  }

  if (is.list(values)) {
    values <- unlist(values, recursive = TRUE, use.names = FALSE)
  }

  if (!length(values)) {
    return(character())
  }

  values <- as.character(values)
  values <- values[!is.na(values)]
  values <- trimws(values)
  values <- stringr::str_replace_all(values, "\\s+", " ")
  values <- values[nzchar(values)]
  unique(values)
}

#' Map raw affiliations to canonical values using a lookup.
#'
#' @param affiliations Character vector of raw affiliation strings.
#' @param lookup Tibble returned by \code{canonicalize_affiliation_values()}.
#'
#' @return Character vector of canonical affiliation names.
#'
#' @noRd
map_canonical_affiliations <- function(affiliations, lookup) {
  if (is.null(affiliations) || !length(affiliations) || !nrow(lookup)) {
    return(character())
  }

  matches <- lookup$canonical[match(affiliations, lookup$original)]
  matches <- matches[!is.na(matches)]
  unique(matches)
}

#' Split a sequence of indices into evenly sized groups.
#'
#' @param n Total length of the sequence.
#' @param size Maximum size for each group.
#'
#' @return List of integer index vectors.
#'
#' @noRd
split_indices <- function(n, size) {
  if (n <= 0) {
    return(list())
  }

  split(seq_len(n), ceiling(seq_len(n) / max(1, size)))
}

#' Extract the canonical institution from an LLM response.
#'
#' Handles structured objects, data frames, lists, and simple character strings.
#'
#' @param result Response object returned by the LLM.
#'
#' @return Character scalar representing the canonical institution.
#'
#' @noRd
extract_canonical_value <- function(result) {
  if (is.null(result)) {
    return("")
  }

  if (is.data.frame(result) && "canonical_institution" %in% names(result)) {
    return(result$canonical_institution[[1]] %||% "")
  }

  if (is.list(result) && "canonical_institution" %in% names(result)) {
    return(result$canonical_institution %||% "")
  }

  if (is.character(result) && length(result)) {
    value <- result[[1]]
    value <- stringr::str_remove(value, "^[A-Za-z_ ]+[:=]\\s*")
    return(value)
  }

  ""
}
#' Normalize canonical affiliation values.
#'
#' Runs the full normalization pipeline twice to enforce idempotence and apply
#' synonym lookups in between passes.
#'
#' @param values Character vector of canonical affiliation strings.
#'
#' @return Character vector of normalized affiliation names.
#'
#' @noRd
finalize_canonical_values <- function(values) {
  normalized <- normalize_institution_name(values)
  normalized <- apply_synonym_lookup(normalized)
  normalize_institution_name(normalized)
}

#' Normalize a vector of institution names.
#'
#' Applies \code{normalize_single_institution()} element-wise.
#'
#' @param values Character vector of institution names.
#'
#' @return Character vector of normalized names.
#'
#' @noRd
normalize_institution_name <- function(values) {
  if (is.null(values)) {
    return(character())
  }

  purrr::map_chr(values, normalize_single_institution)
}

#' Normalize a single institution name.
#'
#' Applies a sequence of transformations that remove diacritics, collapse
#' whitespace, drop sub-units, and harmonise known entities.
#'
#' @param value Institution name as returned by the LLM or data source.
#'
#' @return A single normalized institution name or `NA_character_`.
#'
#' @noRd
normalize_single_institution <- function(value) {
  if (is.null(value) || is.na(value)) {
    return(NA_character_)
  }

  val <- trimws(as.character(value))
  if (!nzchar(val)) {
    return(val)
  }

  val <- stringi::stri_trans_general(val, "Latin-ASCII")
  if (grepl("\\[NA\\]", val, fixed = TRUE)) {
    return(NA_character_)
  }

  val <- stringr::str_replace_all(val, "\\s+", " ")
  val <- expand_common_abbreviations(val)
  val <- remove_leading_subunits(val)
  val <- strip_trailing_subunits(val)
  val <- enforce_university_campus_format(val)
  val <- normalize_uc_campuses(val)
  val <- remove_company_suffixes(val)
  val <- normalize_dwr_name(val)
  val <- normalize_cuny(val)
  val <- normalize_nasa(val)
  val <- enforce_noaa_expansion(val)
  val <- strip_noaa_suffix(val)
  val <- enforce_university_suffix(val)
  val <- normalize_country_names(val)
  val <- normalize_usda_entities(val)
  val <- drop_trailing_country(val)
  val <- normalize_ampersands(val)
  val <- normalize_university_spacing(val)
  val <- normalize_known_institutions(val)
  val <- stringr::str_replace_all(val, ",\\s*,", ", ")
  val <- trimws(val)
  val
}

#' Expand common abbreviations in institution names.
#'
#' @param value Institution name string.
#'
#' @return Character string with abbreviations expanded.
#'
#' @noRd
expand_common_abbreviations <- function(value) {
  replacements <- list(
    "(?i)\\bUniv\\.?\\b" = "University",
    "(?i)\\bInst\\.?\\b" = "Institute",
    "(?i)\\bDept\\.?\\b" = "Department",
    "(?i)\\bIntl\\.?\\b" = "International",
    "(?i)\\bUniv\\.? of" = "University of",
    "(?i)\\bLab\\.?\\b" = "Laboratory",
    "(?i)\\bMt\\.(?=\\s|$)" = "Mount",
    "(?i)\\bMt\\b" = "Mount"
  )

  purrr::reduce(names(replacements), function(acc, pattern) {
    stringr::str_replace_all(acc, pattern, replacements[[pattern]])
  }, .init = value)
}

#' Remove leading organisational sub-units from an institution name.
#'
#' @param value Institution name string.
#'
#' @return Institution name with leading units such as departments removed.
#'
#' @noRd
remove_leading_subunits <- function(value) {
  pattern <- "(?i)^(Department|Dept|Division|School|College|Center|Centre|Institute|Laboratory|Lab|Office|Section|Program|Bureau|Delta Modeling Section|Center for|Centre for|Institute for)[^,]*,\\s*"
  repeat {
    new_val <- stringr::str_replace(value, pattern, "")
    if (identical(new_val, value)) {
      break
    }
    value <- new_val
  }
  value
}

#' Remove trailing organisational sub-units from an institution name.
#'
#' @param value Institution name string.
#'
#' @return Institution name without trailing departments, colleges, etc.
#'
#' @noRd
strip_trailing_subunits <- function(value) {
  trimmed <- stringr::str_replace(
    value,
    "(?i)^(.*?university[^,]*)(?:,?\\s*(College|School|Department|Division|Institute|Center|Faculty|Program).*)$",
    "\\1"
  )

  trimmed <- stringr::str_replace(
    trimmed,
    "(?i)^(State University of New York)(?:.*)$",
    "\\1"
  )

  trimws(trimmed)
}

#' Standardise campus formatting for university names.
#'
#' Replaces dash-separated campus qualifiers with comma-separated forms.
#'
#' @param value Institution name string.
#'
#' @return Institution name with consistent campus formatting.
#'
#' @noRd
enforce_university_campus_format <- function(value) {
  value <- stringr::str_replace_all(
    value,
    stringr::regex("(?i)(University of [^,\\r\\n]+?)[-–]\\s*([A-Za-z0-9&.'() ]+)"),
    "\\1, \\2"
  )
  value
}

#' Drop corporate suffixes from institution names.
#'
#' @param value Institution name string.
#'
#' @return Institution name without trailing company designators.
#'
#' @noRd
remove_company_suffixes <- function(value) {
  value <- stringr::str_replace_all(
    value,
    stringr::regex(",?\\s+(LLC|L\\.L\\.C\\.|Co\\.?|Company|Ltd\\.?|Limited|Inc\\.?|Incorporated|Corporation|Corp\\.?)(?:\\.|,)?(?=$|\\s|\\.)", ignore_case = TRUE),
    ""
  )
  trimws(value)
}

#' Normalize variants of the California Department of Water Resources.
#'
#' @param value Institution name string.
#'
#' @return Canonicalised DWR institution name when applicable.
#'
#' @noRd
normalize_dwr_name <- function(value) {
  if (!nzchar(value)) {
    return(value)
  }

  if (stringr::str_detect(value, stringr::regex("California Department of Water Resources", ignore_case = TRUE))) {
    return("California Department of Water Resources")
  }

  if (
    stringr::str_detect(value, stringr::regex("Department of Water Resources", ignore_case = TRUE)) &&
      stringr::str_detect(value, stringr::regex("(California|Sacramento|CA\\b)", ignore_case = TRUE))
  ) {
    return("California Department of Water Resources")
  }

  if (stringr::str_detect(value, stringr::regex("^Department of Water Resources$", ignore_case = TRUE))) {
    return("California Department of Water Resources")
  }

  value
}

#' Normalize references to CUNY.
#'
#' @param value Institution name string.
#'
#' @return `"City University of New York"` when `CUNY` is detected; otherwise
#'   the input value.
#'
#' @noRd
normalize_cuny <- function(value) {
  if (!nzchar(value)) {
    return(value)
  }

  if (stringr::str_detect(value, stringr::regex("\\bCUNY\\b", ignore_case = TRUE))) {
    return("City University of New York")
  }

  value
}

#' Normalize references to NASA.
#'
#' @param value Institution name string.
#'
#' @return `"National Aeronautics and Space Administration"` when detected.
#'
#' @noRd
normalize_nasa <- function(value) {
  if (!nzchar(value)) {
    return(value)
  }

  if (stringr::str_detect(value, stringr::regex("\\bNASA\\b", ignore_case = TRUE))) {
    return("National Aeronautics and Space Administration")
  }

  value
}

#' Enforce canonical suffixes for known universities.
#'
#' @param value Institution name string.
#'
#' @return Canonical institution name when a suffix rule matches.
#'
#' @noRd
enforce_university_suffix <- function(value) {
  if (!nzchar(value)) {
    return(value)
  }

  rules <- institution_suffix_rules()
  lower_value <- tolower(value)
  match_idx <- match(lower_value, names(rules))

  if (!is.na(match_idx)) {
    return(rules[[match_idx]])
  }

  value
}

#' Mapping of lower-cased institution names to canonical suffix forms.
#'
#' @return Named character vector used by \code{enforce_university_suffix()}.
#'
#' @noRd
institution_suffix_rules <- function() {
  stats::setNames(
    c(
      "Cornell University",
      "Rutgers University"
    ),
    tolower(c("Cornell", "Rutgers"))
  )
}

#' Apply canonical synonyms to a set of institution names.
#'
#' @param values Character vector of institution names.
#' @param synonyms Tibble with `pattern`, `canonical`, and `ignore_case` columns.
#'
#' @return Character vector with synonym replacements applied.
#'
#' @noRd
apply_synonym_lookup <- function(values, synonyms = canonical_synonyms()) {
  if (!length(values) || !nrow(synonyms)) {
    return(values)
  }

  result <- values
  for (i in seq_len(nrow(synonyms))) {
    pattern <- synonyms$pattern[i]
    canonical <- synonyms$canonical[i]
    ignore_case <- isTRUE(synonyms$ignore_case[i])

    matches <- stringr::str_detect(
      result,
      stringr::regex(pattern, ignore_case = ignore_case)
    )

    result[matches & !is.na(result)] <- canonical
  }

  result
}

#' Synonym table used by \code{apply_synonym_lookup()}.
#'
#' @return Tibble of regex patterns, canonical replacements, and case flags.
#'
#' @noRd
canonical_synonyms <- function() {
  tibble::tibble(
    pattern = c(
      "^Scripps Institution of Oceanography.*$"
    ),
    canonical = c(
      "Scripps Institution of Oceanography"
    ),
    ignore_case = TRUE
  )
}

#' Expand NOAA abbreviations to the full agency name.
#'
#' @param value Institution name string.
#'
#' @return Institution name with NOAA references expanded.
#'
#' @noRd
enforce_noaa_expansion <- function(value) {
  if (!nzchar(value) || !stringr::str_detect(value, stringr::regex("NOAA|National Oceanic", ignore_case = TRUE))) {
    return(value)
  }

  replacements <- list(
    "(?i)\\bNOAA\\b" = "National Oceanic and Atmospheric Administration",
    "(?i)National Oceanic and Atmospheric Admin\\.?" = "National Oceanic and Atmospheric Administration"
  )

  val <- purrr::reduce(names(replacements), function(acc, pattern) {
    stringr::str_replace_all(acc, pattern, replacements[[pattern]])
  }, .init = value)

  val
}

#' Normalize ampersands to the word "and".
#'
#' @param value Institution name string.
#'
#' @return Institution name with ampersands replaced.
#'
#' @noRd
normalize_ampersands <- function(value) {
  stringr::str_replace_all(value, " \\& ", " and ")
}

#' Strip trailing NOAA qualifiers that duplicate the agency name.
#'
#' @param value Institution name string.
#'
#' @return Institution name without trailing NOAA subdivisions.
#'
#' @noRd
strip_noaa_suffix <- function(value) {
  if (!stringr::str_detect(value, "(?i)National Oceanic and Atmospheric Administration")) {
    return(value)
  }

  value <- stringr::str_replace(value, "(?i)(National Oceanic and Atmospheric Administration).*", "\\1")
  value <- stringr::str_replace_all(value, "(?i)Administrationistration", "Administration")
  value
}

#' Remove trailing country descriptors from institution names.
#'
#' @param value Institution name string.
#'
#' @return Institution name without trailing country text.
#'
#' @noRd
drop_trailing_country <- function(value) {
  stringr::str_replace(value, ",\\s*(USA|United States of America)$", "")
}

#' Normalize excess punctuation and spacing in university names.
#'
#' @param value Institution name string.
#'
#' @return Institution name with condensed spacing.
#'
#' @noRd
normalize_university_spacing <- function(value) {
  if (!stringr::str_detect(value, "(?i)university")) {
    return(value)
  }

  value <- stringr::str_replace_all(value, "[,–-]+", " ")
  value <- stringr::str_replace_all(value, "\\s+", " ")
  trimws(value)
}

#' Normalize known institutions using explicit regex replacements.
#'
#' @param value Institution name string.
#'
#' @return Canonical institution name when a known pattern matches.
#'
#' @noRd
normalize_known_institutions <- function(value) {
  replacements <- list(
    "(?i)^AECOM$" = "AECOM",
    "(?i)^Jet Propulsion Laboratory.*$" = "California Institute of Technology",
    "(?i)^ICF International$" = "ICF",
    "(?i)^IIT@MIT$" = "Massachusetts Institute of Technology",
    "(?i)^Cal State East Bay$" = "California State University, East Bay",
    "(?i)^Leibniz[-\\s]?Institute of Freshwater Ecology and Inland Fisheries$" = "Leibniz Institute of Freshwater Ecology and Inland Fisheries",
    "(?i)^Metropolitan Water District$" = "Metropolitan Water District of Southern California",
    "(?i)^The Metropolitan Water District of Southern California$" = "Metropolitan Water District of Southern California",
    "(?i)^CalTrout$" = "California Trout",
    "(?i)^NSW Government$" = "New South Wales Government",
    "(?i)^National Center for Agro[-\\s]?Meteorology$" = "National Center for Agro Meteorology",
    "(?i)^National Institute of Meteorological Sciences/Korea Meteorological Administration$" = "National Institute of Meteorological Sciences",
    "(?i)^National Weather Service$" = "United States National Weather Service",
    "(?i)^National Oceanic and Atmospheric Administration$" = "United States National Oceanic and Atmospheric Administration",
    "(?i)^Nevada Seismological Laboratory University of Nevada Reno$" = "University of Nevada Reno",
    "(?i)^Penn State University$" = "Pennsylvania State University",
    "(?i)^Power China Huadong Engineering$" = "PowerChina",
    "(?i)^Purdue University West Lafayette$" = "Purdue University",
    "(?i)^Rutgers University Newark$" = "Rutgers University",
    "(?i)^San Jose State University Research Foundation$" = "San Jose State University",
    "(?i)^Scripps$" = "Scripps Institution of Oceanography",
    "(?i)^Southern California Coastal Water Research Project Authority$" = "Southern California Coastal Water Research Project",
    "(?i)^Stony Brook University State University of New York$" = "Stony Brook University",
    "(?i)^United States Department of Agriculture Natural Resources Conservation Service$" = "United States Department of Agriculture",
    "(?i)^United States Department of Agriculture Agricultural Research Service$" = "United States Department of Agriculture",
    "(?i)^Universita[_\\s]+de[_\\s]+Bourgogne$" = "Universita de Bourgogne",
    "(?i)^Universit\\s*extAllCaps\\{e\\}extAllCaps\\{e\\}\\s+Toulouse\\s+III\\s*[-–]?\\s*Paul\\s+Sabatier$" = "Universite Toulouse III",
    "(?i)^University$" = "University College Dublin",
    "(?i)^University at Buffalo$" = "University at Buffalo State University of New York",
    "(?i)^University for Atmospheric Research$" = "University Corporation for Atmospheric Research",
    "(?i)^University of California Cooperative Extension$" = "University of California Agriculture and Natural Resources",
    "(?i)^University of California at Davis$" = "University of California Davis",
    "(?i)^University of California at Merced$" = "University of California Merced",
    "(?i)^University of Colorado at Boulder$" = "University of Colorado Boulder",
    "(?i)^University of Illinois at Urbana Champaign$" = "University of Illinois Urbana Champaign",
    "(?i)^University of North Carolina Chapel Hill$" = "University of North Carolina at Chapel Hill",
    "(?i)^Wageningen University and Research$" = "Wageningen University",
    "(?i)^Xi'an University$" = "Xi'an University of Technology",
    "(?i)^Bureau of Reclamation$" = "United States Bureau of Reclamation",
    "(?i)^Bureau of Land Management$" = "United States Bureau of Land Management",
    "(?i)^USGS Pacific Coastal and Marine Science Center$" = "United States Geological Survey",
    "(?i)^United States Army RDECOM$" = "United States Army Engineer Research and Development Center",
    "(?i)^European Centre for Medium-Range Forecasts$" = "European Centre for Medium-Range Weather Forecasts",
    "(?i)^European Centre for Medium-Range Weather Forecasting$" = "European Centre for Medium-Range Weather Forecasts",
    "(?i)^Florida A&M University[-–]Florida State University.*$" = "Florida A&M University",
    "(?i)^State University of New York College of Environmental Science and Forestry$" = "State University of New York",
    "(?i)^The Graduate Center,? CUNY$" = "City University of New York",
    "(?i)^The Graduate Center of the City University of New York$" = "City University of New York",
    "(?i)^The Graduate Center City University of New York$" = "City University of New York",
    "(?i)^Johns Hopkins University Applied Physics Laboratory$" = "Johns Hopkins University",
    "(?i)^Monterey Bay Aquarium Research Institute$" = "Monterey Bay Aquarium",
    "(?i)^Monterey Bay Aquarium Institute$" = "Monterey Bay Aquarium"
  )

  purrr::reduce(names(replacements), function(acc, pattern) {
    stringr::str_replace(acc, pattern, replacements[[pattern]])
  }, .init = value)
}
#' Normalize abbreviated country references to their full form.
#'
#' @param value Institution name string.
#'
#' @return Institution name with United States references expanded.
#'
#' @noRd
normalize_country_names <- function(value) {
  value <- stringr::str_replace_all(value, "(?<![A-Za-z])U\\.S\\.(?![A-Za-z])", "United States")
  value <- stringr::str_replace_all(value, "(?<![A-Za-z])US(?![A-Za-z])", "United States")
  value
}

#' Normalize known USDA entities to their canonical agency names.
#'
#' @param value Institution name string.
#'
#' @return Institution name with USDA entities canonicalized.
#'
#' @noRd
normalize_usda_entities <- function(value) {
  replacements <- list(
    "(?i).*(USDA[\\s-]*ARS|United States Department of Agriculture[,\\s-]*(Agricultural Research Service|ARS)).*" = "United States Department of Agriculture",
    "(?i).*National Clonal Germplasm Repository.*" = "United States Department of Agriculture",
    "(?i).*United States Department of Agriculture.*Forest Service.*" = "United States Forest Service",
    "(?i).*USDA.*Forest Service.*" = "United States Forest Service",
    "(?i).*USDA.*Natural Resources Conservation Service.*" = "United States Department of Agriculture",
    "(?i).*Agricultural Research Service,?\\s*US Department of Agriculture.*" = "United States Department of Agriculture",
    "(?i).*NASA.*(Goddard|Ames).*" = "National Aeronautics and Space Administration",
    "(?i).*National Aeronautics and Space Administration.*(Goddard|Ames).*" = "National Aeronautics and Space Administration"
  )

  purrr::reduce(names(replacements), function(acc, pattern) {
    stringr::str_replace(acc, pattern, replacements[[pattern]])
  }, .init = value)
}

#' Normalize University of California campus names that use "at".
#'
#' @param value Institution name string.
#'
#' @return Institution name with "at" replaced by a space.
#'
#' @noRd
normalize_uc_campuses <- function(value) {
  if (!nzchar(value)) {
    return(value)
  }

  stringr::str_replace_all(
    value,
    stringr::regex("\\bUniversity of California at\\s+", ignore_case = TRUE),
    "University of California "
  )
}
