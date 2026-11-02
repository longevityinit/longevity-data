# Repository instructions

## Workflow and verification

- Edit source in `src/`; rebuild generated artifacts in the ignored `output/` directory. Never fix a generated chart without updating its source.
- Keep changes focused. Separate behavior changes from broad formatting or refactoring where practical. Leave unrelated working-tree changes alone.
- Name branches by purpose, using prefixes such as `feat/`, `fix/`, and `chore/`.
- Prefer existing dependencies and browser capabilities. Explain new dependencies, update the appropriate lockfile, and preserve licence notices.
- Update relevant commands, methodology, and output descriptions when behavior changes. See [development](docs/development.md), [architecture](docs/architecture.md), and [headline graph](docs/headline-graph.md) documentation rather than duplicating their details here.
- Local builds must not require storage credentials or upload data. Publication is an explicit separate operation; preserve its provenance checks and recovery behavior. Never commit secrets.

Run applicable checks from the repository root:

| Command | Purpose |
| --- | --- |
| `.venv/bin/python -m unittest discover -s tests -v` | Python calculations, pipeline, build, and publication regression tests |
| `npm test` | JavaScript unit tests using Node's built-in test runner |
| `npm run lint` | ESLint checks for the configured headline modules, tests, and browser configuration |
| `npm run format:check` | Read-only Prettier checks with the same file scope as the formatter |
| `npm run test:browser` | Playwright browser interaction tests |
| `npm run format` | Apply Prettier to the files configured in `package.json`; this modifies files, so use during implementation, not a read-only review |

JavaScript development checks require Node.js 22.13+ and npm. Use `npm ci` for locked dependencies; Playwright browser setup is documented in the development guide. Static chart builds do not require a JavaScript bundler. Follow `eslint.config.js`, `.prettierrc.json`, and the current npm scripts; do not invent competing formatting rules. Their configured coverage is not necessarily repository-wide. Python style should follow existing conventions; no Python formatter or type checker is prescribed here.

Choose checks based on the change, and report anything not run. Test observable behavior and meaningful boundaries rather than mirroring implementation. Add regression coverage for substantive bugs; avoid arbitrary coverage targets and tests for trivial formatting changes.

## Continuous integration

- `.github/workflows/checks.yml` runs Python tests, JavaScript tests, lint, formatting checks, and Chromium fixture tests on pull requests and pushes to `main`; manual runs are also available.
- Keep CI on standard GitHub-hosted Ubuntu runners, which are free for this public repository. Preserve timeouts and cancellation of superseded runs. Avoid paid runners and unnecessary artifact or cache storage.
- Keep publication, production-data downloads, repository secrets, and bucket credentials out of this checks workflow. Use read-only repository permissions and do not persist checkout credentials.
- Browser checks need only deterministic fixture observations and the pinned D3 asset. Verify its checksum; do not run the production download/build pipeline to prepare browser tests.
- Keep documented local commands and CI checks aligned. Report local verification separately from an actual successful GitHub Actions run.

## Reviews

Apply these guidelines to every code, branch, and pull request review in this repository. Scale the depth to the changes. Review the current revision and distinguish verified bugs, maintenance concerns, and optional preferences. A review request does not authorize implementing changes or posting feedback externally.

## Priorities

1. Preserve existing behavior. Check the original download, standardisation, chart build, and publication flows when affected; do not examine only the new feature.
2. Verify calculations independently where practical. Check units, source definitions, population weights, thresholds, missing values, ties, date boundaries, aggregate exclusion, and sex isolation. Matching arithmetic does not establish that source definitions are scientifically compatible. Clearly separate verified results from unresolved methodology questions.
3. Review consistency and maintainability as well as functional correctness. Explain concrete consequences rather than relying on stylistic preferences.

## JavaScript, HTML, and CSS

- Prefer semantic native HTML controls with custom styling where suitable. Handwritten HTML and vanilla JavaScript with D3 are valid choices; do not recommend a framework merely because code is custom.
- Assess custom interactions for keyboard and touch operation, focus behavior, accessible names and states, responsive behavior, and missing or empty data. Pay particular attention to coordinated range controls, tooltips, and crowded chart labels.
- Judge file size by responsibilities, shared mutable state, coupling, and testability, not an arbitrary line limit. Recommend modules around meaningful responsibilities such as state, controls, and chart rendering; avoid unnecessary fragmentation.
- Prefer explicit application state and a consistent update path. Check URL parsing and serialization, persistence, defaults, and synchronization with controls.
- Keep calculations and state transformations independent of DOM operations where practical. Keep ownership clear so multiple modules do not independently mutate the same UI state.
- Handle asynchronous failures explicitly. Do not silently swallow errors unless the fallback is intentional and documented. Use `textContent` for data-derived text instead of inserting untrusted HTML.
- Preserve the distinction between missing values and zero; avoid truthiness-based defaults that conflate them.
- Look for excessive work in frequent event handlers: repeated data indexing, full DOM reconstruction, hidden table rebuilding, and unnecessary renders during dragging or resizing. Distinguish measured performance problems from potential inefficiencies.
- Check event and observer lifecycle, error handling, and whether rerendering loses focus or interaction state.
- Check naming, formatting, dead code, duplicate CSS, repeated constants, and styling consistency. Use the existing ESLint and Prettier tools.
- If files are extracted or assets added, verify the standalone build, relative asset paths, licence notices, and publication manifest include them.

## Python

- Separate computation from file access, network requests, and publication so calculations can be tested independently.
- Validate external data at ingestion boundaries. Errors should identify the relevant dataset, location, year, or field. Preserve missing values distinctly from zero and reject non-finite numeric values where inappropriate.
- Catch specific exceptions. Broad catches belong at deliberate application boundaries and must preserve useful diagnostics. Never use `assert` for required input validation, because assertions can be disabled.
- Avoid mutable default arguments, unnecessary mutable global state, and unexpected mutation of caller-owned data.
- Use context managers for files and explicit encodings for text. Use atomic replacement for critical pointers and design interruption and retry behavior deliberately.
- Add type hints to public functions and complex data structures where they clarify contracts; do not require an unrelated annotation rewrite.

## Validation and reporting

- Run checks appropriate to the change. Python tests do not validate JavaScript behavior. For frontend behavior changes, use focused JavaScript or browser checks where available and report gaps honestly.
- Prioritize meaningful coverage: URL state round trips, selection and clearing, year boundaries, missing data, keyboard interaction, responsive layout, and existing flow regression checks.
- For visual changes, inspect the rendered page and available mockups when relevant. Do not claim a mockup comparison without viewing the reference.
- Lead with actionable findings, including location, trigger, consequence, and a suggested direction. Keep architectural recommendations separate from demonstrated defects. Do not manufacture findings to fill a review.
- State the reviewed revision, what was tested, material limitations, and whether files were changed. Respect explicitly deferred scope and leave unrelated working-tree changes alone.
