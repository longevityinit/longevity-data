# Implementation plan

The first milestone is a reproducible, validated headline life-expectancy graph
embedded in WordPress. This checklist tracks completed setup and proposed
implementation work. See [architecture.md](architecture.md) for the
current system, proposed layers, and definition choices.

## 1. Agree on the chart and audit sources

- [ ] Confirm the written specification versus the mockup's different series.
- [ ] Define country/territory eligibility, the annual population threshold,
      frontier ties, global median, and income-group aggregation and membership.
- [ ] Audit UN WPP and OWID artifacts for female/male life expectancy at birth
      and 65, annual population, income groups, units, and historical coverage.
- [ ] Choose and record source releases, citations, licences, and permitted uses.
- [ ] Assemble a small verified sample spanning countries, years, sexes, and ages.
- [ ] Record missing-data rules and keep historical estimates separate from projections.

Done when the series definitions and source mapping are documented and sample
values can be traced back to the source.

## 2. Make the pipeline reliable locally

- [x] Derive the repository root independently of `.env` discovery.
- [x] Add `--local` to all stages, with data and preview assets isolated in ignored `output/`.
- [ ] Separate publication into an explicit command with upload recovery.
- [x] Document Python setup and record resolved dependency versions for Python 3.10/Linux.
- [ ] **Optional:** restore a selected snapshot from storage, verify its checksum,
      and rebuild without fetching newer upstream data. Not required for initial local development.
- [ ] Fix snapshot upload retries so local completion cannot suppress remote recovery.
- [ ] Reject or explicitly align mismatched multipart CSV headers.
- [ ] Track metadata-only upstream changes as well as CSV changes.
- [ ] Add focused regression tests for upload failure/retry and CSV schema mismatch.

Done when a local build requires no cloud credentials and failed publication
can be retried without deleting or re-downloading a valid snapshot.

## 3. Build standardised data and indicators

- [ ] Implement the selected source adapter and explicit sex/age/measure schema.
- [ ] Add stable location IDs, population references, and versioned income classifications.
- [ ] Validate dimension uniqueness, required units, join coverage, and missing values.
- [ ] Calculate country, frontier, median, and income-group series.
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

- [ ] Annual changes and ten-year improvement rates, with explicit window rules.
- [ ] Country-to-frontier gaps and selected-country comparisons.
- [ ] Country dashboards consuming the shared indicators.
- [ ] Validate IHME provenance before adding healthy-life-expectancy measures.
- [ ] Add life-table inputs for lifespan percentiles and disparity measures.
