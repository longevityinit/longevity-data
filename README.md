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
the pipeline with `--local`. Local data, metadata, and chart previews are written
to the gitignored `output/` directory.

For cloud uploads, copy `env.example` to `.env` in the repository root and fill
in the four `B2_*` settings. Keep credentials in `.env`, which is gitignored.
Omitting `--local` retains the scripts' automatic-upload behavior.

## Repository layout

- `src/pipeline/`: Python ingestion, standardisation, chart builders, and templates.
- `src/charts/`: browser JavaScript maintained by this project.
- `tests/`: automated regression tests.
- `docs/`: architecture, implementation plan, and development instructions.
- `data/`: normal pipeline data; selected provenance metadata is tracked.
- `output/`: fully ignored local data and chart builds, including vendor libraries.

Normal upload-enabled builds still generate charts in ignored `charts/` and
retain the existing publication paths. Edit source in `src/`, not generated
copies in `output/` or `charts/`.
