# Life expectancy at age 65

Sources, calculation rules, and shared chart behavior for the age-65 graph.
The graph shows remaining years of life, separately for women and men.
For build and preview commands, see [Local development](development.md#age-65-graph).

## Sources and coverage

OWID provides separate charts for
[female](https://ourworldindata.org/grapher/remaining-life-expectancy-at-different-ages-females)
and [male](https://ourworldindata.org/grapher/remaining-life-expectancy-at-different-ages-males)
remaining life expectancy. Their `.csv` and `.metadata.json` exports were inspected
on 2026-11-02. Metadata identifies Human Mortality Database (2025) and UN World
Population Prospects (2024), processed by OWID.

| Field | Female | Male |
| --- | --- | --- |
| CSV column | `Age 65` | `Age 65` |
| OWID variable ID | 1118552 | 1118559 |
| Unit and definition | Remaining period life expectancy, years | Remaining period life expectancy, years |
| Non-missing observations | 20,804 | 20,804 |
| Entities, including aggregates | 264 | 264 |
| Observed year range | 1751–2023 | 1751–2023 |

Counts describe the exports inspected on that date, including aggregates. The adapter
maps CSV display headers to metadata keys and validates them. Historical country
coverage is sparse; the full year range is not available for every location.

Sample exported values, in remaining years:

| Entity | Year | Female | Male |
| --- | --- | --- | --- |
| Sweden | 1751 | 11.26 | 10.63 |
| Sweden | 2023 | 22.2336 | 19.5451 |
| Germany | 2023 | 21.3324 | 18.1764 |
| World | 2023 | 18.9811 | 16.0083 |
| Europe (UN) | 2023 | 20.8956 | 17.4829 |

World (`OWID_WRL`) and all six existing UN regional IDs are present. Income groups
and other aggregates are also present upstream. The chart uses the same country
and territory eligibility rules as the birth chart, and offers UN regions separately.
These are source observations, not independently validated life-table calculations.

The chart uses the remaining-years exports directly. OWID's
[female birth chart processing notes](https://ourworldindata.org/grapher/womens-life-expectancy-at-birth)
explain that some age-specific presentations add the starting age to show expected
age at death. The exports used here already contain remaining years, so the adapter
does not subtract 65.

## Calculations

- **Percentiles:** weights use the population of the selected sex aged 65 or older,
  summed from the 65–69 group through 100+. The birth chart uses all ages instead.
  Missing modern country weights cause an error. Countries without life expectancy
  are excluded, and population shares are calculated from the remaining observations.
  Percentiles use the inverse weighted CDF, without interpolation between countries.
- **Best practice:** the highest remaining life expectancy among countries and
  territories with total population strictly above one million. It starts in 1850,
  with at least two eligible locations per year and sex. Ties include all winners.
  The cutoff uses total population, not the number of people aged 65 or older.
- **World and regions:** the source aggregates are used directly, rather than
  calculated by averaging national life expectancies.

Historical population estimates use present-day borders, which may differ from
historical mortality territories. This limits the interpretation of early Best
practice values. Historical coverage, source rights, and the interpolation decision
are tracked in the [implementation plan](plan.md).

## Shared chart behavior

Both graphs use the same URL state, selector, timeline, axes, endpoint labels,
tooltips, data table, dialogs, and styles. The details are described in
[Headline graph](headline-graph.md). The age-65 chart has its own title, measure
labels, source links, methodology, and saved-selection key. Switching sex updates
the source link to the corresponding female or male OWID export.

The indicator calculations group observations by age, sex, and year. The age-65
adapter supplies remaining-life-expectancy observations and sex-specific 65+
population weights to those shared calculations.

Standardised data are written to `output/data/standardised/owid/headline_age65/`;
the chart is written to `output/charts/headline-life-expectancy-age-65/`.
Each chart's publication manifest includes its shared assets and licence notices.
Snapshot dependencies are pinned, and publication requires committed provenance.
The default Pages snapshot set remains the birth chart's five datasets; the two
age-65 exports are downloaded explicitly.
