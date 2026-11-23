# Age-65 graph: source audit and implementation direction

Audit date: 2026-11-02. The local age-65 graph is implemented on the shared headline renderer.
It has not been published or embedded in WordPress.

## Sources verified

OWID provides separate charts for
[female](https://ourworldindata.org/grapher/remaining-life-expectancy-at-different-ages-females)
and [male](https://ourworldindata.org/grapher/remaining-life-expectancy-at-different-ages-males)
remaining life expectancy. Their `.csv` and `.metadata.json` exports were inspected
for this audit. Metadata identifies Human Mortality Database (2025) and UN World
Population Prospects (2024), processed by OWID.

| Field | Female | Male |
| --- | --- | --- |
| CSV column | `Age 65` | `Age 65` |
| OWID variable ID | 1118552 | 1118559 |
| Unit and definition | Remaining period life expectancy, years | Remaining period life expectancy, years |
| Non-missing observations | 20,804 | 20,804 |
| Entities, including aggregates | 264 | 264 |
| Observed year range | 1751–2023 | 1751–2023 |

Counts describe the audited exports, not an approved country universe. CSV display
headers differ from the metadata keys, so an adapter must map and validate them
explicitly. The range does not imply complete historical country coverage.

Sample exported values, in remaining years:

| Entity | Year | Female | Male |
| --- | --- | --- | --- |
| Sweden | 1751 | 11.26 | 10.63 |
| Sweden | 2023 | 22.2336 | 19.5451 |
| Germany | 2023 | 21.3324 | 18.1764 |
| World | 2023 | 18.9811 | 16.0083 |
| Europe (UN) | 2023 | 20.8956 | 17.4829 |

World (`OWID_WRL`) and all six existing UN regional IDs are present. Income groups
and other aggregates are also present upstream; retain the existing explicit
country/region eligibility rules rather than treating every export entity as a country.
These are source observations, not independently validated life-table calculations.

Use these remaining-years exports rather than assuming every OWID life-expectancy
chart uses the same convention. OWID's
[female birth chart processing notes](https://ourworldindata.org/grapher/womens-life-expectancy-at-birth)
explain that some age-specific presentations add the starting age to show expected
age at death. Do not subtract 65 from the remaining-years exports audited here.

## Decisions before calculations

- **Percentile weights:** the agreed age-65 rule uses sex-specific population
  aged 65+, summed from 65–69 through 100+. The birth chart retains all-age
  sex-specific weights. Missing modern country weights cause an error, and missing
  life expectancy is excluded from each percentile with weights renormalized.
- **Best practice:** retains the current total-population >1 million rule, the
  1850 start, and a minimum of two eligible countries per year and sex. Historical
  geographic compatibility remains subject to review before publication.
- **World and regions:** use the age-65 source aggregates directly; do not derive
  them by averaging national life expectancies.
- **Interpolation:** the inverse weighted CDF remains the implemented method.
  Midpoint interpolation is still an open project decision for both graphs.
- **History:** inspect eligible country counts and population joins before adopting
  a historical start date. Birth-chart coverage rules do not establish age-65 coverage.
- **Release and rights:** pin the eventual CSV and metadata snapshots, preserve
  upstream citations, and verify HMD/UN reuse terms for the chosen release before
  publication. This audit does not complete that review.

## Reuse boundaries

Keep vanilla JavaScript, D3, native controls, and the current module boundaries.
The implementation adds a small chart configuration:

- Share URL/state transitions, selection/search, timeline, label placement,
  tooltip positioning, lazy tables, responsive layout, and styles.
- Configure age/measure, title/subtitle, axis and value labels, tooltip/table
  accessible names, coverage notes, defaults, output paths, and persistence keys.
  Current birth-specific text and storage names must not leak into age 65.
- Generalize Python summaries to group by sex **and age**, and accept explicit
  population weights. Keep source-specific validation in adapters.
- Reuse snapshot/provenance/publication utilities. Both charts must package all
  shared assets and licence notices in their own publication manifests.

The age-65 adapter feeds the shared calculations, which group by age as well as
sex and year. A shared builder packages the same JS/CSS and a separate age-65
methodology fragment. `headline-config.js` supplies wording and storage keys. Preserve the birth graph's outputs and interaction
behavior with regression tests. No new frontend framework or bundler is needed.

Local outputs use `headline_age65` for standardised data and
`headline-life-expectancy-age-65` for the chart. Source exports and generated
artifacts remain in ignored `output/`. Snapshot manifests pin all dependencies;
publishing still requires committed provenance. See [development.md](development.md#age-65-graph)
for commands.
