# Implementation plan

The first milestone is a reproducible, validated headline life-expectancy graph
embedded in WordPress. This checklist distinguishes implemented work from
methodology and publication work still to complete. See [architecture.md](architecture.md)
and [headline-graph.md](headline-graph.md) for the current design and definitions.

## 1. Headline data and definitions

- [x] Import OWID female/male life expectancy, historical observations, total
      population, and sex-specific population snapshots.
- [x] Use stable location codes and explicit sex, age, and unit fields.
- [x] Calculate Best practice using total population strictly above one million,
      retain ties, start in 1850, and require two eligible locations per sex/year.
- [x] Use OWID's World series rather than a country median; exclude income groups.
- [x] Calculate sex-specific population-weighted p10, p25, p75, and p90 using
      the inverse weighted CDF; keep projections out of the historical chart.
- [x] Validate inputs and test thresholds, ties, missing values, aggregate exclusion,
      and sex isolation with known examples.
- [ ] Decide whether percentile calculations should use midpoint interpolation.
- [ ] Review compatibility of historical mortality territories with population
      estimates based on current borders before publication.
- [ ] Complete final source-to-output sample checks and confirm the chart's
      definitions and presentation with the project stakeholders.

## 2. Pipeline reliability

- [x] Derive the repository root independently of `.env` discovery.
- [x] Always build in ignored `output/`, without cloud credentials or uploads.
- [x] Require matching committed snapshot provenance before publication.
- [x] Separate publication into an explicit command with upload recovery.
- [x] Record resolved Python dependencies and document local setup.
- [x] Make snapshot publication retries independent of importer completion.
- [x] Repair the headline snapshot pointer when reusing a dated download.
- [x] Add regression tests for upload failure/retry and publication validation.
- [ ] Reject or explicitly align mismatched multipart CSV headers and add regression coverage.
- [ ] Track metadata-only upstream changes as well as CSV changes.
- [ ] **Optional:** restore a selected stored snapshot, verify its checksum, and
      rebuild without fetching newer upstream data.

## 3. Headline chart and checks

- [x] Render selected countries, UN regions, Best practice, World, and percentiles;
      omit the unselected grey background lines.
- [x] Add sex selection, searchable line selection, shared URL state, year-range
      controls, responsive labels, and hover/pinned tooltips.
- [x] Include methodology, coverage notes, source/licence credits, CSV/JSON downloads,
      and an on-demand data table.
- [x] Add loading, empty-data, and error states.
- [x] Separate state, controls, rendering, and styles; add JS and Chromium behavior tests.
- [x] Configure independent Python, JS/style, and fixture-only Chromium CI jobs.
- [ ] Confirm the latest GitHub Actions run passes; local tests alone do not establish this.
- [ ] Complete final mobile, keyboard, touch, and contrast review before publication.

## 4. Age-65 graph

Sources and calculation rules: [age-65 graph](age-65-graph.md).

- [x] Identify OWID female/male remaining-life-expectancy exports and inspect
      their age-65 units, historical coverage, World, and regional observations.
- [x] Use sex-specific 65+ percentile weights and retain total-population Best practice eligibility.
- [x] Add pinned source snapshots and validate country/year/sex coverage against
      population inputs, including early historical observations.
- [x] Configure shared chart code for age, measure, title, axis/tooltip/table text,
      sources, defaults, storage key, and output ID while preserving the birth chart.
- [x] Generalize indicator calculations around age and explicit weight inputs;
      avoid copying the entire headline pipeline.
- [x] Test age isolation, source values, remaining-years units, weights, missing data,
      shared controls, asset packaging, and both publication manifests.

- [ ] Review age-65 historical country coverage and population joins, including
      compatibility between present-day population borders and mortality territories.
- [ ] Confirm HMD/UN reuse terms and citations for the selected age-65 releases
      before publication.

## 5. Publish and embed

- [ ] Package and validate a release of immutable data, metadata, and rendering assets.
- [ ] Validate uploaded artifacts before updating the public release pointer.
- [ ] Check content types, caching, cross-origin access, and download links.
- [ ] Embed in WordPress and verify sizing and interaction.
- [ ] Exercise rollback and rebuilding from recorded snapshots and code revision.

## Later milestones

- [ ] Decide the default behavior for WordPress embeds: should parameter-free embeds show the same fixed view to everyone, or personalize using saved preferences and country detection? The current implementation uses a fixed iframe default; this is provisional. Explicit URL state should continue to reproduce the requested view.
- [ ] Annual changes and ten-year improvement rates, with explicit window rules.
- [ ] Country-to-frontier gaps and selected-country comparisons.
- [ ] Country dashboards consuming the shared indicators.
- [ ] Validate IHME provenance before adding healthy-life-expectancy measures.
- [ ] Add life-table inputs for lifespan percentiles and disparity measures.
