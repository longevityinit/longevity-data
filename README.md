# The Longevity Initiative’s data to charts pipeline

This repo is the first tentative steps towards a reproducible-build longevity data pipeline for [The Longevity Initiative](https://thelongevityinitiative.org/).

If you want to help out, please [get in touch](https://thelongevityinitiative.org/contact/)!

## Project documentation

- [Architecture](docs/architecture.md): current pipeline and proposed design.
- [Implementation plan](docs/plan.md): phased checklist, starting with the headline life-expectancy graph.

- [Local development](docs/development.md): environment setup, pinned dependencies, and verification.

## Local configuration

Local builds need no storage credentials or `.env` file. Follow the
[local development guide](docs/development.md) to install dependencies and run
the pipeline. Local data, metadata, and chart previews are written
to the gitignored `output/` directory.

For cloud uploads, copy `env.example` to `.env` in the repository root and fill
in the four `B2_*` settings. Keep credentials in `.env`, which is gitignored.
Build stages never upload. Use `python src/pipeline/publish.py --manifest <path>`
to publish explicitly; see the development guide for dry runs and recovery.

## Repository layout

- `src/pipeline/`: Python ingestion, standardisation, chart builders, and templates.
- `src/charts/`: browser JavaScript maintained by this project.
- `tests/`: automated regression tests.
- `docs/`: architecture, implementation plan, and development instructions.
- `data/`: reviewed snapshot provenance, required in Git before publication.
- `output/`: fully ignored local data and chart builds, including vendor libraries.

All generated files live in `output/`. Edit source in `src/`. Use
`record_metadata.py --manifest <path>` to prepare provenance for review and
commit; publication verifies it matches Git before accessing the bucket.
