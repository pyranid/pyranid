# Releasing Pyranid

## Release verification

Use JDK 25 for the publication build so the generated Javadocs include their bundled font resources. The published library's Java 17 baseline is unchanged.

Before cutting a release, run the [development verification checks](CONTRIBUTING.md#development-verification) from the `pyranid/` project directory, including the relevant integration suites.

To verify the release profile without signing or uploading artifacts:

```bash
mvn -Pcoverage,release -Dgpg.skip=true clean verify
```

## Signing and publishing

Artifact signing and Maven Central publishing are isolated in the `release` profile. Use `mvn -P release deploy` only when publishing a release or snapshot; normal local and CI `verify` runs do not require GPG credentials. Automatic publishing is intentionally disabled: after deployment validation succeeds, review and publish the deployment in the Central Publisher Portal.
