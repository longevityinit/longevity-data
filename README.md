# The Longevity Initiative’s data to charts pipeline

This repo is the first tentative steps towards a reproducible-build longevity data pipeline for [The Longevity Initiative](https://thelongevityinitiative.org/).

If you want to help out, please [get in touch](https://thelongevityinitiative.org/contact/)!

## Project documentation

- [Architecture](docs/architecture.md): current pipeline and proposed design.
- [Implementation plan](docs/plan.md): phased checklist, starting with the headline life-expectancy graph.

## Local configuration

Copy `env.example` to `.env` in the repository root and fill in the four `B2_*`
storage settings. Keep credentials in `.env`, which is gitignored.

Current scripts require this file and automatically upload their outputs to
the configured bucket. Separating local builds from publication is planned;
see the implementation checklist above. Python dependencies are listed in
`requirements.txt`.
