# DWR Publication Inventory
`dwrpubs` builds a curated inventory of peer-reviewed publications connected to the California Department of Water Resources. It combines manually maintained DOI lists with Crossref and OpenAlex fetchers, uses LLM-assisted steps for author affiliation canonicalization and discipline classification, and produces ready-to-use datasets for reports and dashboard visualizations.

## LLM-Assisted Processing
- Affiliation cleanup: [`data-raw/all_metadata.R`](data-raw/all_metadata.R) optionally calls Gemini models to canonicalize institution names pulled from Crossref and OpenAlex, falling back to cached lookups when API credentials are absent.
- Discipline tagging: [`data-raw/classified_inventory.R`](data-raw/classified_inventory.R) batches article titles and abstracts through Gemini for taxonomy-based classification, storing the resulting labels, explanations, and confidence scores.
- Author overrides: helper functions in [`R/author_normalization.R`](R/author_normalization.R) can be guided by LLM-normalized name mappings when new spelling variations are detected.

## Roadmap for Future Work
### Documentation
- [ ] Flesh out [`vignettes/generate_inventory.qmd`](vignettes/generate_inventory.qmd) with an end-to-end workflow walkthrough.
- [ ] Add package-level documentation that introduces the data objects and exported helpers.

### Reliability and Automation
- [ ] Create automated tests for package functions.
- [ ] Wrap the [`data-raw/`](data-raw/) scripts in a reproducible refresh pipeline that handles API credentials.
- [ ] Configure CI to run `R CMD check` and guard the data and package refresh process.

### Products
- [ ] Create a visualization dashboard and other communications products.

## License
Released under the [MIT License](LICENSE.md).
