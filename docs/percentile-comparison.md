# Lower life-expectancy percentile comparison

The headline preview now shows both population-weighted 10th and 25th percentile
candidates, separately for women and men, for 1950–2023. Neither is selected as
the final lower reference line yet.

## Definition

Sort country/territory life-expectancy values ascending. The percentile is the
first value whose cumulative population reaches 10% or 25% of the total covered
population (inverse weighted empirical CDF). At an exact boundary, use the value
that reaches the boundary. Do not interpolate. Ties yield the same value regardless
of ordering; JSON records all locations matching the threshold value.

Use female population for women and male population for men. These weights are
summed from exhaustive UN WPP 2024 five-year age groups (0–4 through 95–99,
plus 100+) in OWID exports, joined by location code and year. Missing weights
fail the build; there is no fallback to total population. Include small countries and territories;
the one-million threshold applies only to best practice. Exclude source aggregates.
Missing life expectancy is excluded and the denominator is the remaining covered
population. JSON records covered population and country count for each candidate.
Before 1950, leave values missing because the available countries are not globally
representative. This is neither a percentile of individual ages at death nor the
average life expectancy within the lowest population fraction.

## Initial comparison

Calculated from the life-expectancy/historical snapshots dated 2026-09-13 and two sex-specific
population snapshots dated 2026-09-22. Reproduce by running
`src/pipeline/indicators/headline.py`; values are in the generated `headline.json`.
Mean absolute annual change is mean(abs(value[t] - value[t-1])) over 1951–2023.
It describes variation, not measurement error: genuine mortality events, changes
in country rankings and population weights all contribute.

| Sex | Percentile | 2023 value (years) | Mean absolute annual change | Largest absolute annual change |
| --- | --- | ---: | ---: | --- |
| Female | 10th | 67.536 | 0.825 | -7.118 years, 1960 |
| Female | 25th | 73.268 | 0.693 | +4.146 years, 2022 |
| Male | 10th | 62.609 | 0.905 | +6.455 years, 1961 |
| Male | 25th | 68.720 | 0.760 | +8.161 years, 1962 |

The 25th percentile has lower average annual variation for both sexes in this
sample, but the men's series has a larger single-year jump. Smoothness alone
does not choose the measure; inspect changes in threshold countries and mortality
before describing jumps as noise. No smoothing has been applied.

## Published precedents

- [New York Fed, 2019: Does U.S. Health Inequality Reflect Income Inequality—or Something Else?](https://libertystreeteconomics.newyorkfed.org/2019/10/does-us-health-inequality-reflect-income-inequalityor-something-else/)
  uses population-weighted county life-expectancy distributions and discusses
  their 25th and 75th percentiles. This is a close conceptual precedent at a
  different geographic scale, not a ready-made global series.
- [Heterogeneity in Disparities in Life Expectancy Across US Metropolitan Areas](https://pmc.ncbi.nlm.nih.gov/articles/PMC9574908/)
  uses differences/ratios between the 90th and 10th population-weighted percentiles
  of census-tract life expectancy within metropolitan areas. Its
  [published supplement](https://cdn-links.lww.com/permalink/ede/b/ede_1_1_2022_07_28_mahl_ede21-0805_sdc1.pdf)
  explicitly identifies the population-weighted percentile measure.

A targeted OWID search did not identify a published global country-level weighted
10th/25th percentile series. That is not proof of absence. OWID supplies our inputs;
these two derived series are our calculations. OWID survival-age percentiles are
a different measure and should not be substituted.

Validation: all 296 sex-specific percentile values independently matched a raw-CSV
integer-weight calculation. Other chart series are unchanged.
