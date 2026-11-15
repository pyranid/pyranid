# Releasing Pyranid

## Release verification

Use JDK 25 for the publication build so the generated Javadocs include their bundled font resources. The published library's Java 17 baseline is unchanged.

Before cutting a release, run the [development verification checks](CONTRIBUTING.md#development-verification) from the `pyranid/` project directory, including the relevant integration suites.

At the start of each release cycle, set `japicmp.baseline.version` in `pom.xml` to the most recent published release. Remove exclusions for APIs already absent from that baseline. Run `mvn verify` against the new baseline before changing public APIs, so compatibility checks also protect APIs introduced in the previous release.

To verify the release profile without signing or uploading artifacts:

```bash
mvn -Pcoverage,release -Dgpg.skip=true clean verify
```

## Release bookkeeping checklist

Keep the development version at `4.9.0-SNAPSHOT` and public installation/download examples at the most recent
published version until the release is authorized. Verification does not authorize signing, uploading, publishing
or deploying the website. The user handles all Git commits; stage changes only when explicitly requested, and
do not run an automatic commit workflow.

For an authorized 4.9.0 release, review these changes together before the user creates the release commit:

- Set the core `pom.xml` release version and rename the CHANGELOG release heading. Preserve the explicit
  `release-4-9-0` anchor so README migration links continue working after `-SNAPSHOT` is removed.
- Update README installation coordinates, release/download examples and the release highlights to the version
  actually being published. Leave historical release sections accurate; `detachLobValues` belongs to 4.9.0.
- Update the website's `src/pages/index.md` installation/download/release examples to that same version.
  Regenerate `public/llms-full.txt` through the website artifact task and review the generated result; do not
  edit generated release-version text independently.
- Check `COMPATIBILITY.md` against the current baseline and migration decisions. For 4.9.0 the baseline is 4.8.0;
  only the exact three-argument ParameterRedactor return-type change is excluded from the checked report.
  Inspect the unfiltered report too. Remove that exception when a later cycle advances past 4.8.0.
- Review the approved Optional-redactor source/binary migration, SQLite timestamp normalization and Oracle
  UUID character-storage migration notes. Run the tested SQLite recipe and legacy-format Oracle controls;
  do not apply a generic timestamp rewrite to an application's production database as a release-build step.
- Include the working `TransactionResult.java` acknowledgment/IN_DOUBT Javadoc correction. Ensure all transaction
  overloads describe pre-commit refusals and the outcome API's corresponding failure/result behavior.
- Run the verification above and the relevant driver integrations, including migration regressions; inspect
  the JAR, sources, Javadocs, API report and dependency scopes from the exact candidate being released.
- Publish 4.9.0-only website documentation together with the 4.9.0 artifact, after publication is explicitly
  authorized. Do not deploy those pages ahead of the corresponding public API. Verify versioned links and
  download/example assets as part of that authorized release.

After an actual release, start the next development cycle explicitly, advance the compatibility baseline to the
published artifact and remove obsolete exceptions. Do not infer those version/baseline changes from a successful
local verification run.

## Signing and publishing

Artifact signing and Maven Central publishing are isolated in the `release` profile. Use `mvn -P release deploy` only when publishing a release or snapshot; normal local and CI `verify` runs do not require GPG credentials. Automatic publishing is intentionally disabled: after deployment validation succeeds, review and publish the deployment in the Central Publisher Portal.
