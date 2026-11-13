# Internal graph review and GitHub Pages

There are three separate operations. Existing `Checks` and bucket/WordPress
publication are unchanged; no bucket credentials or new repository secrets are
needed. Graph builds currently include only the headline at-birth chart, not the
already published original graph or unfinished age-65 work.

| Operation | Trigger | Data | Result |
| --- | --- | --- | --- |
| Prepare graphs | Every push/merge to `main`, or manual on `main` | The snapshot selected in `config/pages-snapshot.json` | Downloadable complete site artifact; nothing deployed |
| Refresh graph data | Manual on `main` | New OWID downloads | Candidate data release and review graphs; existing selection unchanged |
| Deploy graphs | Manual on `main`, with a successful preparation run ID | Exact prepared artifact | Public GitHub Pages site |

## First-time setup

Merge these workflows into `main`. Enable **Settings → Pages → GitHub Actions**.
In **Settings → Environments → github-pages**, allow deployments from `main`.
The first preparation will fail with an actionable message until a snapshot has
been selected. It must not silently substitute current upstream data.

Run **Actions → Refresh graph data → Run workflow** from `main`, then follow the
review/adoption steps below. The selection file is deliberately not initialized
with an invented archive or unreviewed local data.

## Refresh and adopt data deliberately

1. Run **Refresh graph data** from `main`. It downloads the five source exports,
   records checksums and dates, and calculates/builds the candidate graph using
   that commit. Each candidate uses a unique date/run/attempt snapshot version.
2. Download the `data-review-<attempt>` artifact. It contains `graphs.tar.gz` and
   `adopt-snapshot.zip`. Extract the graph archive locally and serve it over HTTP
   to review the graph. Check coverage, changed values, calculations, and source
   metadata before adoption. The candidate is explicitly marked non-deployable;
   it cannot be published with Deploy graphs.
3. Extract `adopt-snapshot.zip` into a clean feature checkout. Review and commit
   only its `config/pages-snapshot.json` and `data/snapshots/` provenance files,
   then open a PR. The archive contains no raw data to add to Git. The selection
   includes the source release tag and an automatically generated SHA-256; no
   person has to calculate, copy, or type a checksum.
4. Merging the reviewed PR adopts the snapshot. **Prepare graphs** then builds a
   deployable site using the adopted data and the new `main` commit.

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

Open **Actions → Deploy graphs → Run workflow** on `main` and enter the successful
**Prepare graphs run ID** shown in its summary or URL. No manual archive upload,
GitHub graph release, or checksum entry is needed. Source-data prereleases are
managed by Refresh graph data, separately from the site artifact.

Deployment checks the run's repository, workflow, branch, event, success,
revision, and attempt before downloading its artifact. It then verifies the
complete site archive and deployability flag before publishing that exact site.
It does not rebuild or download new source data during deployment. Expired,
failed, foreign, refresh-candidate, and non-main runs are rejected. A preparation
or verification failure leaves the existing live site unchanged. Deployments are
serialized. To roll back within retention, select an older successful prepared
run whose artifact still exists.

The deployment job shows the live URL, expected to be
<https://longevityinit.github.io/longevity-data/>. Validate the published graphs
after the first actual deployment; local verification is not a hosted run.

All workflows use standard Ubuntu runners with timeouts. Only refresh can write
repository contents (to preserve its data release); only the deployment job can
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
