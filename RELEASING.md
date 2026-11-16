# Releasing Pyranid

## Release verification

Use JDK 25 for the publication build so the generated Javadocs include their bundled font resources. The published library's Java 17 baseline is unchanged.

Before cutting a release, run the [development verification checks](CONTRIBUTING.md#development-verification) from the `pyranid/` project directory, including the relevant integration suites.

At the start of each release cycle, set `japicmp.baseline.version` in `pom.xml` to the most recent published release. Remove exclusions for APIs already absent from that baseline. Run `mvn verify` against the new baseline before changing public APIs, so compatibility checks also protect APIs introduced in the previous release.

To verify the release profile without signing or uploading artifacts:

```bash
mvn -Pcoverage,release -Dgpg.skip=true clean verify
```

Build the final candidate with network access and without Maven's `-o`/`--offline` flag. The Javadoc plugin
omits configured external links in offline mode, so an otherwise successful offline build does not produce
the intended annotation links. Check the generated JSpecify and JSR-305 annotation links in the final JAR.

## Release bookkeeping checklist

During development, retain the development `-SNAPSHOT` version and keep public installation/download examples
at the most recent published version until release-candidate preparation is authorized. This cycle's candidate is `4.9.0`, with
reproducible-build timestamp `2026-11-16T03:04:44Z`. The release-version references are prepared locally together.
Candidate preparation and verification do not authorize signing, uploading, publishing
or deploying the website. The user handles all Git commits; stage changes only when explicitly requested, and
do not run an automatic commit workflow.

Review these candidate changes together before the user creates the 4.9.0 release commit:

- Set the core `pom.xml` release version and rename the CHANGELOG release heading. Preserve the explicit
  `release-4-9-0` anchor so README migration links continue working after `-SNAPSHOT` is removed.
- Review `project.build.outputTimestamp` in `pom.xml`: choose and record the intended reproducible-build
  timestamp before producing the final candidate, and use it consistently for verification and publication.
- Update README installation coordinates, release/download examples and the release highlights to the version
  actually being published. Leave historical release sections accurate; `detachLobValues` belongs to 4.9.0.
- Update the website's `src/pages/index.md` installation/download/release examples to that same version.
  Regenerate `public/llms-full.txt` through the website artifact task and review the generated result; do not
  edit generated release-version text independently.
- Check `COMPATIBILITY.md` against the current baseline and migration decisions. For 4.9.0 the baseline is 4.8.0;
  only the exact three-argument ParameterRedactor return-type change is excluded from the checked report.
  Inspect the unfiltered report too. Remove that exception when a later cycle advances past 4.8.0.
- Review the approved Optional-redactor source/binary migration, SQLite timestamp normalization and Oracle
  UUID character-storage migration notes, plus MySQL/MariaDB configured-zone storage changes. Use the
  [upgrade checklist](CHANGELOG.md#upgrading-to-490-action-required). Run the tested SQLite recipe and legacy-format Oracle controls;
  do not apply a generic timestamp rewrite to an application's production database as a release-build step.
- Check that all transaction/retry overloads describe pre-commit refusal, strict commit-retry proof and the
  outcome API's corresponding failure/result behavior. Review the generated Javadoc, not only source comments.
- Run the verification above and the relevant driver integrations, including migration regressions; inspect
  the JAR, sources, Javadocs, API report and dependency scopes from the exact candidate being released.
- Prepare Javadoc from that exact candidate's `-javadoc.jar` using the staging procedure below. Keep the
  JSpecify/JSR-305 links and bundled fonts; verify new pages such as `SqlInterval` and `TransactionOutcome`,
  method anchors, local stylesheets and fonts before replacing the documentation site's output.
- Prepare the website export from the reviewed source/version, run its tests and artifact generator, and
  inspect `public/llms-full.txt`, internal links and downloads. Include **new files as well as modified files**
  in the user's release commits: in particular `pyranid.com/public/examples/SqliteTimestampMigration.java`
  must accompany the recipe that links it. Check that it matches the core example and exported download.
- Treat Maven publication, README/version links, Javadoc publication and website deployment as one coordinated
  release window. After explicitly authorized artifact publication is available, publish the matching Javadoc
  and website and push the reviewed README/version changes together. Do not deploy 4.9-only pages ahead of
  their artifact/API. Verify public Javadoc links, version/download links and the SQLite migration download
  after that authorized rollout. A successful local export is not a deployment.

After an actual release, start the next development cycle explicitly, advance the compatibility baseline to the
published artifact and remove obsolete exceptions. Do not infer those version/baseline changes from a successful
local verification run.

## Staging the Javadoc site

Build the candidate with JDK 25 and the release verification command above. Stage the **same verified Javadoc
JAR** that accompanies the library artifact; regenerating documentation from a different working tree does not
establish artifact identity. This preview creates only a temporary directory:

```bash
candidate_javadoc=/absolute/path/to/the/verified/pyranid-X.Y.Z-javadoc.jar
javadoc_preview="$(mktemp -d)"
unzip -q "$candidate_javadoc" -d "$javadoc_preview"
test -s "$javadoc_preview/index.html"
test -s "$javadoc_preview/com/pyranid/SqlInterval.html"
test -s "$javadoc_preview/com/pyranid/TransactionOutcome.html"
```

Inspect links, annotations and font resources in that preview. Only as part of an authorized release, copy the
reviewed preview into the `javadoc.pyranid.com/dist/` checkout and use that site's normal publication workflow.
The user handles its commits too. Use that checkout's README when preparing the release. Do not overwrite the released site's
output merely to verify a snapshot. Record the library, sources and Javadoc SHA-256 digests; if signing/deploy
rebuilds any candidate, compare its digests and rerun relevant checks before the final publication action.

## Signing and publishing

Artifact signing and Maven Central publishing are isolated in the `release` profile. Use `mvn -P release deploy` only when publishing a release or snapshot; normal local and CI `verify` runs do not require GPG credentials. Automatic publishing is intentionally disabled: after deployment validation succeeds, review and publish the deployment in the Central Publisher Portal.
