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
