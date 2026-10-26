# Headline graph

Data coverage, methodology, and display behavior for the headline life-expectancy chart. For build and preview commands, see [Local development](development.md#headline-graph-first-version).

## Sources and coverage

The importer stores five independent OWID snapshots: the modern female/male
export (which includes population and income groups), historical female/male
life expectancy, historical population, and female/male population by age. The indicator stage validates inputs,
joins historical population by location code and year, and calculates summaries.
All five snapshot dependencies are pinned in the publication manifest.

The first version covers 1751–2023 at birth, beginning with Sweden; the available range is derived from the stored observations. Before 1950 it includes only historical
locations that match the modern country/territory universe. Coverage is sparse;
the World series starts in 1950 to avoid presenting the
early sample as globally representative. Recorded population must
be strictly above one million for frontier eligibility; all tied winners are
retained. Best practice starts in 1850 and requires at least two eligible locations with observations for that year and sex; otherwise it remains blank. Country observations with missing historical population remain available individually but are excluded from frontier eligibility. There is no population interpolation; most years before 1800 lack a matching population observation.

The generated JSON records the number of missing historical country-years. Only selected series are plotted. Missing values
are not interpolated. Historical population is based on current borders; its
compatibility with historical mortality territories needs review before publishing.

## Percentiles

Percentile weights use sex-specific population, summing non-overlapping five-year
age groups including 100+. Best-practice eligibility still uses total population.

## Selection and shared URLs

The headline “Show on chart” selector supports countries, source-provided UN regions, and population-weighted percentiles (10, 25, 75, 90). Income groups are excluded from the headline outputs. Selected lines can be removed individually and are saved in browser local storage. Source aggregates without stable location codes are not offered. Percentiles retain the inverse weighted CDF definition; midpoint interpolation remains undecided.

Headline URL state uses `?show=best,world,DEU,p25&sex=f&start=1950&end=2023`. Sex is `f` or `m`. Best practice and World are always included, even for `show=`. Countries retain their codes (e.g. `DEU`); percentiles retain `p10`, `p25`, `p75`, `p90`. Aliases map to internal data IDs:

| URL alias | Data ID |
| --- | --- |
| `best` | `frontier` |
| `world` | `OWID_WRL` |
| `un-africa` | `UN_AFR` |
| `un-asia` | `UN_ASI` |
| `un-europe` | `UN_EUR` |
| `un-latin-america-caribbean` | `UN_LAC` |
| `un-northern-america` | `UN_NAM` |
| `un-oceania` | `UN_OCE` |

Legacy `lines`, internal IDs, `sex=female|male`, and `country=DEU` links remain supported. `show` takes precedence over `lines`, which takes precedence over `country`. Selections can be comma-separated or repeated. URLs are rewritten to the canonical format using `replaceState`, without reloading or adding history entries. URL state overrides saved selections and locale guesses. Unknown IDs are ignored and invalid sex values fall back to female. An iframe without state parameters starts with women, best practice, World, and p10; it ignores local preferences and locale. Standalone views without state may use saved selections or locale, then serialize that resolved view. These URLs reproduce display settings, not a pinned dataset version.

## Timeline and axes

The year-range controls serialize `start` and `end` in the URL. Bounds are clamped to available years; reversed/equal bounds reset to the full range. Sliders use annual steps and marked years, with clickable year markers. Double-click the timeline or press Escape on a focused slider handle to reset the full range. The table and endpoint labels follow the selected range.


The headline chart plots selected series only; unselected countries are not drawn in the background.

## Labels, tooltips, and selector

Endpoint label behavior: endpoint labels prioritize Best practice, World, selected percentiles, and a hovered line. Remaining labels are chosen by their distance from the pointer at the hovered year, using the same principle as the hover popup. Lines without data for that year are omitted. Desktop shows up to ten endpoint labels; charts narrower than 600px show up to four (or enough to retain all selected reference lines). Without a pointer, comparisons are ranked by endpoint value. Labels remain ordered by endpoint height and are spaced to avoid overlap. There is no separate line legend. The inline “Show on chart” panel manages selections: search, category filters, Selected filter, and a clear-comparisons action. At viewport widths of 1100px and above it is a sidebar beside the plot; on narrower screens it sits above the plot. The comparison count and Clear selection action sit below the category filters. On mobile it starts collapsed; expanded lists shrink to their content up to a scrollable maximum height. Every option shows its life expectancy for the displayed end year and sex (a dash when unavailable); the selector does not include line-color swatches. Selections update the chart and URL immediately.

The headline plot trims empty years within the requested range and fits the y-axis to available values of the selected lines and sex. Rounded y-axis bounds include at least two years of padding; the scale does not always start at zero.

Timeline controls use compact editable year fields without visible From/To labels. Double-clicking the timeline or pressing Escape on a range handle resets the range; there is no separate Reset button. Preset labels are hidden on mobile; snapping remains available. Historical coverage is explained below the chart rather than inside the plot. A persistent note below the chart explains the Best practice population cutoff and limited historical coverage. The footer groups View data, CSV, and JSON actions above the source credit and expandable credits/methodology.

Hover previews omit missing values and prioritize reference lines, showing up to ten entries. Clicking the plot pins a scrollable tooltip of selected lines with available observations; missing observations are currently omitted from both hover and pinned views. Endpoint labels are hover-only and the plot has no keyboard focus outline; annual observations are also available through View data.

## Code boundaries

- `src/charts/headline.js`: startup, data loading, one state update path, and animation-frame scheduling.
- `headline-state.js`: pure URL/state transitions, visible data bounds, and tooltip entry selection.
- `headline-controls.js`: selector, year controls, and synchronization from explicit state to the DOM.
- `headline-renderer.js`: D3 drawing, endpoint layout, tooltips, and a lazily populated data table.
- `headline.css`: standalone styles copied and tracked by the publication manifest.

The state is `{ sex, selectedSeries, startYear, endYear }`; controls dispatch
changes rather than owning part of it. Rapid updates are coalesced into one
animation frame. A visible table updates when its state changes; a hidden table
is not rebuilt during slider drags or resizes. The SVG is still recreated for
each scheduled draw; this change does not claim incremental D3 rendering or a
measured performance improvement.
