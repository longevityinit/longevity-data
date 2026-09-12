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
python src/pipeline/download/owid/life_expectancy.py --local
python src/pipeline/standardise/owid/life_expectancy.py --local
python src/pipeline/charts/life_expectancy.py --local
```

`--local` saves artifacts without uploading or loading storage credentials.
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
on each build. Rebuild after changing that source file. Normal runs retain
`data/` and `charts/`, including the existing metadata-tracking policy.

Manual imports also support `--local`:

```bash
python src/pipeline/download/ingest_manual.py --local \
  --source ihme --dataset all_cause_deaths \
  --zip /path/to/export.zip --url 'https://example.org/source-permalink'
```

Omitting `--local` retains the existing automatic-upload behavior and uses
separate inputs in `data/`. Local mode does not read or modify those snapshots.
There is no promotion command from `output/` to published data yet; explicit
publication and upload recovery remain planned work.

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
