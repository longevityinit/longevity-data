# Implementation plan

The first milestone is a reproducible, validated headline life-expectancy graph
embedded in WordPress. This checklist tracks completed setup and proposed
implementation work. See [architecture.md](architecture.md) for the
current system, proposed layers, and definition choices.

## 1. Agree on the chart and audit sources

- [ ] Confirm the written specification versus the mockup's different series.
- [ ] Define country/territory eligibility, the annual population threshold,
      frontier ties, World life expectancy, and income-group aggregation and membership.
- [ ] Audit UN WPP and OWID artifacts for female/male life expectancy at birth
      and 65, annual population, income groups, units, and historical coverage.
- [ ] Choose and record source releases, citations, licences, and permitted uses.
- [ ] Assemble a small verified sample spanning countries, years, sexes, and ages.
- [ ] Record missing-data rules and keep historical estimates separate from projections.

Done when the series definitions and source mapping are documented and sample
values can be traced back to the source.

## 2. Make the pipeline reliable locally

- [x] Derive the repository root independently of `.env` discovery.
- [x] Always build in ignored `output/`; remove the local/non-local distinction.
- [x] Prepare snapshot provenance for review and require matching committed records before publication.
- [x] Separate publication into an explicit command with upload recovery.
- [x] Document Python setup and record resolved dependency versions for Python 3.10/Linux.
- [ ] **Optional:** restore a selected snapshot from storage, verify its checksum,
      and rebuild without fetching newer upstream data. Not required for initial local development.
- [x] Make snapshot publication retries independent of importer completion.
- [ ] Reject or explicitly align mismatched multipart CSV headers.
- [ ] Track metadata-only upstream changes as well as CSV changes.
- [x] Add focused regression tests for upload failure/retry and publication validation.
- [ ] Add focused regression tests for CSV schema mismatch.

Done when a local build requires no cloud credentials and failed publication
can be retried without deleting or re-downloading a valid snapshot.

## 3. Build standardised data and indicators

- [ ] Implement the selected source adapter and explicit sex/age/measure schema.
- [ ] Add stable location IDs, population references, and versioned income classifications.
- [ ] Validate dimension uniqueness, required units, join coverage, and missing values.
- [ ] Calculate country, frontier, World, and income-group series.
- [ ] Preserve frontier winners, input snapshot IDs, and calculation code version.
- [ ] Test the one-million boundary, ties, missing years, aggregate exclusion,
      sex/age isolation, and chosen aggregation rules with small known examples.
- [ ] Cross-check representative output values against source data.

Done when the highlighted series are independently checked and reproducible
from recorded inputs.

## 4. Build the headline graph

- [ ] Define a chart manifest with a stable ID, series metadata, sources, and defaults.
- [ ] Render grey country lines and the four highlighted series with clear labels.
- [ ] Add a female-default, keyboard-accessible sex toggle and informative tooltips.
- [ ] Include methodology, coverage, source attribution, and a data download.
- [ ] Add loading, empty-data, and error states.
- [ ] Check mobile layout, contrast, keyboard access, and an accessible text/table alternative.
- [ ] Validate the at-birth chart, then add the age-65 view with remaining-years units.

Done when the chart matches the agreed definitions, remains readable on mobile,
and its controls and values have been checked.

## 5. Publish and embed

- [ ] Package immutable data, metadata, and rendering assets into a release.
- [ ] Validate uploaded artifacts before updating the public release pointer.
- [ ] Check content types, caching, cross-origin access where needed, and embed behavior.
- [ ] Embed in WordPress and verify sizing, interaction, and download links.
- [ ] Exercise rollback and a rebuild from the recorded snapshot and code revision.
- [ ] Add CI checks and document the update/review/publication workflow.

Done when the live embed works and the release can be rebuilt or rolled back.

## Later milestones

- [ ] Decide the default behavior for WordPress embeds: should parameter-free embeds show the same fixed view to everyone, or personalize using saved preferences and country detection? The current implementation uses a fixed iframe default; this is provisional. Explicit URL state should continue to reproduce the requested view.
- [ ] Decide the placement of the country/region/percentile selector and removable selected-line entries. The current “+ Add line” control below the selected entries is provisional; review its position on desktop and mobile.

- [ ] Annual changes and ten-year improvement rates, with explicit window rules.
- [ ] Country-to-frontier gaps and selected-country comparisons.
- [ ] Country dashboards consuming the shared indicators.
- [ ] Validate IHME provenance before adding healthy-life-expectancy measures.
- [ ] Add life-table inputs for lifespan percentiles and disparity measures.

## Headline first version implemented

- [x] Separate local at-birth preview, female default with native radio controls.
- [x] OWID snapshots for modern values and the requested 1900–1949 history.
- [x] Python frontier calculations and source-provided World series, population threshold and ties.
- [x] Grey country lines, four reference series, year tooltips and annual table.
- [x] CSV/JSON downloads, source information and explicit early-coverage caveats.
- [x] Focused tests for thresholds, ties, sex isolation, missing data and historical joins.
- [ ] Confirm historical geographic comparability, source licences and income-group membership before publication.
- [ ] Complete browser/mobile/accessibility review and independently verify sample values.
- [ ] Add age 65 after reviewing the at-birth graph.
- [ ] Revisit IP-based country suggestions: inspect the WordPress embed and hosting/CDN setup, then decide how to pass a country hint to the chart. Saved selection must take priority, followed by IP country, browser locale, and manual selection. Currently only saved selection, locale guessing, and manual selection are implemented.
- [ ] Revisit the population-weighted 10th percentile definition: compare the current inverse weighted CDF (no interpolation) with linear interpolation between countries' cumulative-population midpoints, using sex-specific weights. Review differences in the actual series before choosing; document the chosen method and test boundaries and ties. The current calculation remains unchanged pending this decision.

- [x] Extend individual historical country series to the earliest available sex-specific observation (Sweden, 1751); retain gaps in reference series and exclude missing population from best-practice eligibility.
- [ ] Review historical population coverage and geographic comparability before publishing the extended frontier.

- [ ] Resolve the existing IHME attribution mismatch: the stored licence/citation file cites GBD 2016, while metadata identifies GBD 2023 (and the query lists YLDs despite the dataset name all_cause_deaths). Verify against the original download before use; do not infer a replacement citation.
