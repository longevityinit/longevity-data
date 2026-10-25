# Local development

## Python environment

This setup is verified with stable Python 3.10 on Linux. The scripts require
Python 3.10 or newer syntax; other versions have not yet been checked with the
pinned dependencies. Node.js is not required for the current static charts.

From the repository root:

```bash
python3 --version
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt -r requirements.lock.txt
```

Use `python3.10` explicitly if `python3` points to another interpreter. Select
`.venv/bin/python` as your editor's interpreter. Activate the environment in each
new shell, or use `.venv/bin/python` directly.

`requirements.txt` declares direct dependency ranges. `requirements.lock.txt`
records resolved direct and transitive versions for this environment. For an
intentional update, install `requirements.txt` in a clean virtual environment,
regenerate the version list using `python -m pip freeze`, and verify it before
replacing the lock file. This list has no package hashes and does not guarantee
compatibility across operating systems.

## Configuration

Local builds and verification need no `.env` file. For bucket access only,
create it if it does not already exist:

```bash
test -e .env || cp env.example .env
```

Fill in the `B2_*` values only when bucket access is needed. `.env` and `.venv/` are gitignored.

## Verify the environment

These checks do not download data or upload anything:

```bash
python -m pip check
python -m compileall -q src
python -c "import boto3, dotenv, requests, xxhash, yaml, pandas; print('Dependency imports OK')"
python src/pipeline/download/ingest_manual.py --help
```

Run the regression tests (fixture downloads; no network or bucket access):

```bash
python -m unittest discover -s tests -v
```

These cover local pipeline execution and storage isolation, not the scientific
validity of upstream data.

## Build locally

Run the stages in order from the repository root:

```bash
python src/pipeline/download/owid/life_expectancy.py
python src/pipeline/standardise/owid/life_expectancy.py
python src/pipeline/charts/life_expectancy.py
```

All build stages save artifacts and publication manifests without bucket access.
All stages always use `output/`; the former `--local` option has been removed.
It is not an offline flag: downloading needs access to OWID, and the chart stage
fetches pinned D3/Observable Plot assets on its first run. Once those assets and
the snapshot are present, standardisation and chart generation can run offline.
All local outputs, including snapshot metadata, live under the fully ignored
`output/` directory:

```text
output/data/snapshots/
output/data/standardised/
output/charts/vendor/
output/charts/lib/longevityplot.js
output/charts/life-expectancy/
```

The chart build copies our tracked `src/charts/longevityplot.js` into the preview
on each build. Rebuild after changing that source file. Tracked provenance in `data/` is updated only by the preparation command below.

Manual imports use the same output directory:

```bash
python src/pipeline/download/ingest_manual.py \
  --source ihme --dataset all_cause_deaths \
  --zip /path/to/export.zip --url 'https://example.org/source-permalink'
```

Builds never modify the tracked snapshots in `data/`.

## Publish explicitly

Each successful stage writes `publish.json` beside its artifacts:

- Snapshot: `output/data/snapshots/<source>/<dataset>/<date>/publish.json`
- Standardised data: `output/data/standardised/<source>/<dataset>/publish.json`
- Chart: `output/charts/life-expectancy/publish.json`

A chart manifest publishes
only the chart and browser assets, not the source snapshot. Publish each stage's
manifest separately if you need its artifacts in the bucket.

Validate a build and inspect its destination keys without credentials:

```bash
python src/pipeline/publish.py --manifest output/charts/life-expectancy/publish.json --dry-run
```

Before uploading, prepare the snapshot metadata for Git review. This works with
snapshot, standardised-data, or chart manifests; derived manifests follow their
hash-pinned dependencies to the source snapshot:

```bash
python src/pipeline/record_metadata.py --manifest output/charts/life-expectancy/publish.json
```

This copies snapshot metadata, any bundled licence, and the selected snapshot
pointer into `data/snapshots/`. It never copies CSVs, stages changes, or commits.
Review the Git diff, stage the intended provenance files, and commit them.
Existing versioned records with different content are rejected; use a new
snapshot version. Updating `current.yaml` is allowed and must also be reviewed.

Snapshot metadata, licences, and frozen pointers are normalized to LF before
manifest hashes are recorded, matching Git normalization on every platform.
Raw CSV and upstream JSON bytes are preserved. Regenerate older manifests and
downstream builds if their provenance used CRLF line endings.

Publishing requires matching bytes in the working tree, Git index, and `HEAD`.
Unrelated uncommitted code does not block it. A chart's source provenance is
checked too, although publishing a chart does not upload its raw snapshot.
The Git pointer means the selected snapshot; the bucket pointer means the
published snapshot. They can differ until publication completes.

Old manifests without provenance links must be regenerated. Rerun
standardisation and the chart build; standardisation also prepares the snapshot
manifest. The dry run above checks artifacts but deliberately does not require
committed provenance, so it can be used before review.

To upload, configure `.env` using `env.example`, then run:

```bash
python src/pipeline/publish.py --manifest output/charts/life-expectancy/publish.json
```

Publishing an `output/` manifest targets the configured bucket's normal object
keys; it is not a separate remote development environment. Use a development
bucket if appropriate. The key needs object read and write permissions.

The publisher checks every file's SHA-256 and size before remote access. It
uploads files with checksum metadata and content types, verifies them using
object HEAD requests, and writes `publish.receipt.json` after each success.
Retry the same command after failure: matching remote objects are skipped;
missing or changed remote objects are uploaded again. Remote state, not the
receipt alone, determines what can be skipped. Objects from older uploads
without checksum metadata are uploaded once by the new publisher.

Snapshot manifests freeze `current.yaml` and publish that pointer last. Retrying
an older snapshot intentionally selects that version; avoid concurrent publishers
targeting the same dataset. Completed existing snapshots can gain a manifest by
rerunning their importer (manual imports can use the original `--date`). No
re-download is needed when the dated snapshot already exists.

Do not edit or rebuild artifacts during publication. If files change, validation
fails; rebuild the manifest before retrying. Manifests, receipts, and frozen
pointer copies are generated and gitignored. Receipts contain no credentials.

Chart HTML is uploaded after its assets, but chart paths are still mutable:
this is resumable publication, not atomic chart releases. Versioned chart assets
and concurrent-publisher coordination remain future work.

A fresh clone contains metadata but no source CSVs or generated charts. The
IHME snapshot is not needed for the OWID chart. Remote snapshot restoration
remains to be implemented.

## Preview

Once chart artifacts and their JavaScript dependencies have been generated,
preview them over HTTP:

```bash
python -m http.server 8000 --bind 127.0.0.1 --directory output/charts
```

Open <http://127.0.0.1:8000/life-expectancy/>. Starting the server does not build
the chart; that URL is unavailable until its output exists. Stop with Ctrl+C.

## Headline graph (first version)

The new chart is separate from the original life-expectancy preview:

```bash
python src/pipeline/download/owid/headline.py
python src/pipeline/indicators/headline.py
python src/pipeline/charts/headline.py
python -m http.server 8000 --bind 127.0.0.1 --directory output/charts
```

Open <http://127.0.0.1:8000/headline-life-expectancy/>. Rebuild after changing
`src/charts/headline.js` or the headline template. No new libraries are required;
the renderer uses the existing pinned D3 asset. The old Plot chart is unchanged.

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
retained. Best practice starts in 1850 and requires at least two eligible locations with observations for that year and sex; otherwise it remains blank. Country observations with missing historical population remain available individually but are excluded from frontier eligibility. There is no population interpolation; most years before 1800 lack a matching population observation. The generated JSON records the number of missing historical country-years. Only selected series are plotted. Missing values
are not interpolated. Historical population is based on current borders; its
compatibility with historical mortality territories needs review before publishing.

The generated JSON records input snapshot versions, source descriptions, and
Git revision/dirty status. A dirty revision is not a reproducible code release;
commit and rebuild before publication. CSV downloads contain standardized input
observations; JSON includes calculated series and frontier winners.

Use `output/charts/headline-life-expectancy/publish.json` with the existing dry-run
and metadata preparation commands. This first version is for local review;
source licences and historical geographic alignment still
need confirmation before public release.

Percentile weights use sex-specific population, summing non-overlapping five-year
age groups including 100+. Best-practice eligibility still uses total population.

The headline “Show on chart” selector supports countries, source-provided UN regions, and population-weighted percentiles (10, 25, 75, 90). Income groups are excluded from the headline outputs. Selected lines can be removed individually and are saved in browser local storage. Source aggregates without stable location codes are not offered. Percentiles retain the inverse weighted CDF definition; midpoint interpolation remains a follow-up in the plan.

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

The year-range controls serialize `start` and `end` in the URL. Bounds are clamped to available years; reversed/equal bounds reset to the full range. Sliders use annual steps and marked years, with clickable year markers. Double-click the timeline or press Escape on a focused slider handle to reset the full range. The table and endpoint labels follow the selected range; the y-axis remains fixed for comparison.


The headline chart plots selected series only; unselected countries are not drawn in the background.

Headline line identification: endpoint labels prioritize Best practice, World, selected percentiles, and a hovered or keyboard-focused line. Remaining labels are chosen by their distance from the pointer at the hovered year, using the same principle as the hover popup. Lines without data for that year are omitted. Desktop shows up to ten endpoint labels; charts narrower than 600px show up to four (or enough to retain all selected reference lines). Without a pointer, comparisons are ranked by endpoint value. Labels remain ordered by endpoint height and are spaced to avoid overlap. There is no separate line legend. The inline “Show on chart” panel manages selections: search, category filters, Selected filter, and a clear-comparisons action. At viewport widths of 1100px and above it is a sidebar beside the plot; on narrower screens it sits above the plot. The comparison count and Clear selection action sit below the category filters. On mobile it starts collapsed; expanded lists shrink to their content up to a scrollable maximum height. Every option shows its life expectancy for the displayed end year and sex (a dash when unavailable); the selector does not include line-color swatches. Selections update the chart and URL immediately.

The headline plot trims empty years within the requested range and fits the y-axis to available values of the selected lines and sex. Rounded y-axis bounds include at least two years of padding; the scale does not always start at zero.

Timeline controls use compact editable year fields without visible From/To labels. Double-clicking the timeline or pressing Escape on a range handle resets the range; there is no separate Reset button. Preset labels are hidden on mobile; snapping remains available. Historical coverage is explained below the chart rather than inside the plot. A persistent note below the chart explains the Best practice population cutoff and limited historical coverage. The footer groups View data, CSV, and JSON actions above the source credit and expandable credits/methodology.
