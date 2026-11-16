# Internal graph review and GitHub Pages

There are three separate operations. Existing `Checks` and bucket/WordPress
publication are unchanged; no bucket credentials or new repository secrets are
needed. Graph builds currently include only the headline at-birth chart, not the
already published original graph or unfinished age-65 work.

| Operation | Trigger | Data | Result |
| --- | --- | --- | --- |
| Prepare graphs | Every push/merge to `main`, or manual on `main` | The snapshot selected in `config/pages-snapshot.json` | Downloadable complete site artifact; nothing deployed |
| Refresh graph data | Manual on `main` | New OWID downloads | Candidate data release and review summary; optional adoption PR; selection unchanged |
| Deploy graphs | Manual on `main`, normally with no inputs | Exact prepared artifact | Public GitHub Pages site |

## First-time setup

Merge these workflows into `main`. Enable **Settings → Pages → GitHub Actions**.
In **Settings → Environments → github-pages**, allow deployments from `main`.
Automatic adoption PRs are **off by default**. Refresh works without permission
to create PRs. Once an administrator enables the setting below, check
**create_pr** when running Refresh graph data to opt in. Only the optional PR job
requests pull-request write permission; it is skipped completely in normal mode.

For automatic adoption PRs, enable **Settings → Actions → General → Workflow
permissions → Allow GitHub Actions to create and approve pull requests**. The
refresh job only creates PRs; it does not approve or merge them. No personal
access token is needed. Organization policy may restrict this setting.

The first preparation will fail with an actionable message until a snapshot has
been selected. It must not silently substitute current upstream data.

Run **Actions → Refresh graph data → Run workflow** from `main`, then follow the
review/adoption steps below. The selection file is deliberately not initialized
with an invented archive or unreviewed local data.

## Refresh and adopt data deliberately

1. Run **Refresh graph data** from `main`. It downloads the five source exports,
   records checksums and dates, and calculates/builds the candidate graph using
   that commit. Each candidate uses a unique date/run/attempt snapshot version.
2. With **create_pr** unchecked (the default), ask a maintainer to prepare the
   adoption PR from the generated review artifact, or extract `adopt-snapshot.zip`
   into a clean checkout and commit its selection/provenance files through a PR.
   Refresh succeeds without trying to push a branch or open a PR.
   With **create_pr** checked, open the adoption PR linked in the workflow summary. It contains the generated
   selection/provenance files and a comparison against the previous selected data:
   changed values, added/removed rows, missing values, and the largest numeric
   change. The baseline and candidate use the same calculation code, isolating
   data changes from code changes.
3. Review that summary and the source metadata. Download the review graph artifact
   if a visual comparison is useful. If GitHub shows **Approve workflows to run**,
   approve the PR's checks and wait for them to pass. Token-created PR checks can
   require this approval; no extra workflow-dispatch permissions are granted.
4. Merge the PR when satisfied. In automatic mode, no ZIP extraction, manual
   provenance copying, or branch creation is required. Neither mode needs manual
   checksum entry. The merge adopts the data and
   triggers **Prepare graphs**. Nothing is deployed until you run **Deploy graphs**.

The `data-review-<attempt>` artifact retains the review graph, adoption ZIP, and
comparison report for 14 days. The ZIP remains a recovery option if repository
policy prevents automatic PR creation. If explicitly opted-in PR creation is blocked, the optional PR job reports that error instead
of claiming success; its data release, pushed branch, and review artifact remain
available. Each refresh creates a separate candidate PR; close obsolete ones to
avoid unintentionally adopting an older candidate later. Checksums remain
machine-generated and verified internally.

Source CSVs and upstream metadata are preserved together in `snapshot.tar.gz` on
an automatically created, versioned GitHub prerelease named
`graph-data-<run>-<attempt>`. These source archives are public in this public
repository even though Pages has not been deployed. Confirm source licensing
before running the first refresh. Historical geography questions remain in the
methodology. Internal-review wording does not make a Pages site private.
Do not replace assets on a selected release or delete an adopted data release.
The selected archive is hash-checked and its source checksums are verified on
each restore. Data releases are durable; they do not use expiring Actions
artifacts as the source of truth. Remove rejected candidates when no longer needed.
A candidate's successful calculations and checksum checks do not establish its
scientific validity; that is part of the adoption review.

## Prepare on merges

`Prepare graphs` reads the committed selection, downloads that archived snapshot
from this repository's release, and restores it into a fresh output directory.
It does not fetch source data from OWID. Browser libraries may be downloaded by
the existing chart builder. Indicators are recalculated from the restored data;
chart HTML/JS/CSS are rebuilt from the clean source commit during packaging.
The existing committed-provenance checks remain required for deployable builds.
The homepage displays the snapshot identifier and code commit.

The complete site is saved as `graphs-<attempt>` for **14 days**. Download and
extract `graphs.tar.gz` from that artifact to preview it with:

```bash
python -m http.server 8001 --bind 127.0.0.1 --directory /path/to/extracted/site
```

The graph and vendor relative layout supports both `/` and the GitHub project
subpath `/longevity-data/`. The package includes source/dependency notices, not
raw snapshots, bucket credentials, receipts, or unrelated chart directories.
If the artifact expires, manually rerun Prepare graphs using the same selected
data. That uses the chosen workflow commit; an old code release requires a
separately reviewed build rather than silently claiming to reproduce it.

## Deploy a chosen prepared build

Open **Actions → Deploy graphs → Run workflow** on `main`, leave the optional
**build_run** field blank, and run it. Deployment selects the latest preparation
run for the exact `main` commit at dispatch time. It requires that run to have
succeeded: an absent, failed, cancelled, or still-running preparation stops with
an instruction to prepare/wait, rather than falling back to older code or an
earlier successful run. If `main` changes while selection/download is in progress,
it stops and asks you to run deployment again. Pagination covers all matching
runs. The selected run ID, attempt, and commit appear in the job summary.

For an intentional rollback, enter an older successful **Prepare graphs run ID**
in the optional field. This explicitly permits an older commit. The same
repository/workflow/branch/event/success checks still apply.

Deployment verifies the site archive and deployability flag before publishing
that exact prepared artifact. It does not rebuild or fetch source data. Expired,
failed, foreign, refresh-candidate, and non-main builds are rejected. Expired or
missing artifacts require a new preparation; deployment never silently chooses
a different artifact. Preparation/verification failure leaves the live site
unchanged. Deployments are serialized. No graph release, archive upload, checksum
entry, or normal build-ID entry is needed.

The deployment job shows the live URL, expected to be
<https://longevityinit.github.io/longevity-data/>. Validate the published graphs
after the first actual deployment; local verification is not a hosted run.

All workflows use standard Ubuntu runners with timeouts. Only refresh can write
repository contents to preserve candidate releases. Only the optional adoption
job can push a data branch and open a PR; it cannot dispatch workflows. Only the deployment job can
write Pages and request its identity token. Checkout credentials are not
persisted. Site/review artifacts expire after 14 days; the Pages transfer
artifact after one day. Keep artifact and durable release storage modest.

## Local pipeline equivalents

With a clean checkout and the selected snapshot archive downloaded locally:

```bash
python src/pipeline/pages_snapshot.py restore --archive output/snapshot.tar.gz
python src/pipeline/indicators/headline.py
python src/pipeline/charts/headline.py
python src/pipeline/pages.py build --manifest output/charts/headline-life-expectancy/publish.json
```

A restore requires a fresh `output/data/snapshots` directory so it cannot mix
existing pointers with the selected snapshot. Preserve any needed local output
before preparing a separate checkout. Builds never modify tracked provenance.

[GitHub custom Pages workflow documentation](https://docs.github.com/en/pages/getting-started-with-github-pages/using-custom-workflows-with-github-pages).

For an optional PR-job retry, rerun all jobs so the review artifact and run-attempt
identifier stay aligned. Normal refreshes leave `create_pr` unchecked.
