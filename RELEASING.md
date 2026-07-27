# Releasing event-outboxer

Runbook for cutting a release to Maven Central (Sonatype Central Portal)
and GitHub. The Maven / CI infrastructure is already wired — see the
`release` profile in `pom.xml` and `.github/workflows/release.yml`; this
document is the human checklist around it.

## Contents

1. [One-time setup](#one-time-setup)
2. [Per-release procedure](#per-release-procedure)
3. [Troubleshooting](#troubleshooting)

---

## One-time setup

Done once per repository / maintainer. Skip if already configured.

### 1. Sonatype Central account

1. Register at <https://central.sonatype.com> using "Sign in with GitHub".
2. Because the groupId is `io.github.bams22`, the namespace is
   auto-verified against the `bams22` GitHub account — no manual DNS
   or JIRA ticket required.
3. Generate a user token: Portal → top-right avatar → **View Account**
   → **Generate User Token**. Copy both the `username` (opaque string,
   not the human login) and the `password`. These become the GitHub
   secrets `CENTRAL_USERNAME` and `CENTRAL_TOKEN`.

### 2. GPG key for artifact signing

Maven Central requires every artifact to be PGP-signed.

```bash
# generate a 4096-bit RSA key; skip expiry so the maintainer key is
# stable. Name it after the GitHub identity used for releases.
gpg --full-generate-key

# find the key id — the 16-hex-digit value on the `sec` line
gpg --list-secret-keys --keyid-format LONG

# publish the public key to the keyservers Central consults
gpg --keyserver keys.openpgp.org        --send-keys <KEY_ID>
gpg --keyserver keyserver.ubuntu.com    --send-keys <KEY_ID>

# export the private key as an ASCII-armored block, one-shot for
# GitHub secrets. Do NOT commit this file.
gpg --armor --export-secret-keys <KEY_ID> > private.asc
```

### 3. GitHub repository secrets

Settings → Secrets and variables → Actions → **New repository secret**:

| Secret            | Source                                           |
|-------------------|--------------------------------------------------|
| `CENTRAL_USERNAME`| Sonatype user token `username` (from step 1).   |
| `CENTRAL_TOKEN`   | Sonatype user token `password` (from step 1).   |
| `GPG_PRIVATE_KEY` | Entire contents of `private.asc` including the `-----BEGIN PGP PRIVATE KEY BLOCK-----` and `-----END ...-----` lines. |
| `GPG_PASSPHRASE`  | Passphrase chosen when generating the GPG key. Required even if empty (the workflow reads it unconditionally). |

Delete the local `private.asc` after uploading to avoid leaving the
private key on disk.

### 4. Verify locally (optional but recommended)

Before the first real release, run the dry-run command described in
the per-release procedure below. That catches GPG, sources-jar and
javadoc-jar issues without creating a tag.

---

## Per-release procedure

Versions are **CI-friendly** (every module declares
`<version>${revision}</version>`): the concrete value is injected at
release time from the git tag, and `flatten-maven-plugin` writes
`.flattened-pom.xml` files with the value resolved before deploy. The
`<revision>` property in `pom.xml` stays on a `-SNAPSHOT` value and is
never edited for releases — the tag drives everything.

Assume we're cutting `0.2.0`. Substitute your target version throughout.

### 1. Pre-flight checks

- [ ] `main` branch is green in the `CI` workflow (unit matrix 17/21/25
      plus the Testcontainers IT job).
- [ ] `CHANGELOG.md` contains a `## [0.2.0] — YYYY-MM-DD` section with
      Added / Changed / Removed / Fixed bullets for everything in this
      release. (Commit this on `main` first if missing.)
- [ ] All feature commits are merged into `main`.
- [ ] Working tree is clean (`git status`).

### 2. (Optional but recommended) Local dry-run

Stages every artifact to `./stage/` so you can inspect the files that
would be uploaded. Fails fast on GPG / sources / javadoc misconfig.
Pass `-Drevision=` so the flattened poms carry the release version:

```bash
./mvnw -B -ntp -Prelease clean deploy \
  -Drevision=0.2.0 \
  -DaltDeploymentRepository=local::file:./stage \
  -DskipTests
```

Verify under `./stage/io/github/bams22/*/0.2.0/`:

- [ ] `*.jar`, `*-sources.jar`, `*-javadoc.jar`, `*.pom`
- [ ] A matching `.asc` GPG signature next to each of the above.
- [ ] The `.pom` files contain `<version>0.2.0</version>`, not
      `${revision}` — confirms `flatten-maven-plugin` ran.

Delete `./stage/` after inspection.

### 3. Tag and push

```bash
git tag -a v0.2.0 -m "Release 0.2.0"
git push origin v0.2.0
```

That's the entire release action on your side. Pushing the `v*` tag
triggers `.github/workflows/release.yml`, which:

1. Strips the leading `v` from the tag name to derive
   `-Drevision=0.2.0`.
2. Extracts the `## [0.2.0]` section from `CHANGELOG.md`. **The job
   fails fast here** if the section is missing — this runs before
   anything is deployed, so a tag without a matching CHANGELOG entry
   publishes nothing at all.
3. Runs `./mvnw clean verify -Drevision=0.2.0` — the full unit-test
   suite must pass on the release runner before anything leaves it.
4. Runs `./mvnw -Prelease deploy -Drevision=0.2.0 -DskipTests` (tests
   already ran in the previous step) on a JDK 25 GitHub Actions
   runner. `flatten-maven-plugin` produces `.flattened-pom.xml` with
   the resolved version; `central-publishing-maven-plugin` uploads it
   plus jars and `.asc` signatures to Sonatype Central.
5. Creates a GitHub Release at tag `v0.2.0` with the extracted notes.

There is no "Release X.Y.Z" or "Bump to X.Y.(Z+1)-SNAPSHOT" commit in
git history — the tag itself marks the release.

### 4. Watch the release workflow

Actions tab → **Release** workflow → most recent run.

Expected duration: 5–10 minutes. On success, the workflow has:

1. Staged a deployment in **VALIDATED** state at
   <https://central.sonatype.com/publishing/deployments>. The workflow
   does **not** auto-publish (`<autoPublish>false</autoPublish>` in
   the parent pom) so the operator has a last chance to abort.
2. Extracted the `## [0.2.0]` section from `CHANGELOG.md` and created
   a **GitHub Release** at `v0.2.0` with those notes — visible under
   Releases on the repo. If this step fails because the changelog had
   no section for this version, the whole job fails before the
   release is announced.

### 5. Promote to Maven Central

1. Open <https://central.sonatype.com/publishing/deployments>.
2. Find the deployment under namespace `io.github.bams22`, version
   `0.2.0`.
3. Status should be `VALIDATED`. If `FAILED`, open the deployment
   details, fix the reported issue, drop the deployment, delete the
   tag and GitHub Release, then restart from step 3 with a new patch
   version (Central does not allow re-uploading the same GAV).
4. Click **Publish**. Artifacts become visible on Maven Central within
   15–30 minutes:
   - <https://repo1.maven.org/maven2/io/github/bams22/event-outboxer-spring-boot-starter/0.2.0/>
   - <https://central.sonatype.com/artifact/io.github.bams22/event-outboxer-spring-boot-starter>

### 6. (Optional) Roll the SNAPSHOT property forward

The `<revision>` property in `pom.xml` defaults `-SNAPSHOT` builds.
Rolling it forward is cosmetic — nothing in the release pipeline
depends on it — but keeps local snapshot coordinates aligned with the
next planned version:

```xml
<revision>0.3.0-SNAPSHOT</revision>
```

Commit with a fresh `## [Unreleased]` section added above `[0.2.0]`
in `CHANGELOG.md` so subsequent feature commits have somewhere to
land their notes.

---

## Troubleshooting

### `versions:set` fails with `NoClassDefFoundError: org/codehaus/stax2/util/StreamReader2Delegate`

`versions-maven-plugin` 2.19.0+ pulls `woodstox-core` 7.x onto its
classpath but keeps `stax2-api` at 4.2.2, which doesn't provide the
class woodstox 7.x expects — every invocation crashes on startup.
The parent POM pins the plugin to `2.16.2` (last release before the
broken combo). If you see this error, you either bypassed the pin
with a fully-qualified invocation or the pin was removed — restore
`<versions-maven-plugin.version>2.16.2</versions-maven-plugin.version>`
in `pom.xml` or call the pinned version explicitly:

```bash
./mvnw org.codehaus.mojo:versions-maven-plugin:2.16.2:set \
  -DnewVersion=N.N.N -DgenerateBackupPoms=false
```

### `401 Unauthorized` during Maven deploy

The `CENTRAL_TOKEN` GitHub secret is stale or was copied incompletely.
Generate a new user token in the Central Portal (Account → **Generate
User Token**) and update the secret. Tokens do not expire automatically
but can be revoked.

### `gpg: signing failed: No secret key` / `PGP signature not found`

The `GPG_PRIVATE_KEY` secret is malformed. The most common mistake is
copying only the base64 body without the `-----BEGIN PGP PRIVATE KEY
BLOCK-----` and `-----END ...-----` delimiter lines. Re-export with:

```bash
gpg --armor --export-secret-keys <KEY_ID>
```

and paste the full output (including delimiters) into the secret.

### `sources.jar is missing` / `javadoc.jar is missing`

The release profile didn't activate. Confirm the CI workflow invokes
`./mvnw -Prelease deploy` — without the `-Prelease` flag the
`maven-source-plugin` and `maven-javadoc-plugin` executions are
skipped.

### Validation errors about SCM / license / developers

Central requires every POM to have `<scm>`, `<licenses>`,
`<developers>`, `<url>`, `<name>`, `<description>`. These are in the
parent POM and inherited by every module; if a module overrides them
incorrectly the validation fails. Restore the parent values.

### "Deployment with the same coordinates already exists"

Each Maven Central GAV is one-shot. If the release must be re-cut,
increment the patch version (`0.1.0` → `0.1.1`) rather than trying to
overwrite. Update the changelog accordingly.

### The workflow succeeded but nothing is in Central after an hour

Check whether the deployment is stuck in `VALIDATED` waiting for
manual **Publish**. With `<autoPublish>false</autoPublish>` the
operator must promote it explicitly (step 6 above). Flip
`<autoPublish>` to `true` in the parent POM if you prefer fully
automatic promotion — most maintainers keep it `false` as an abort
gate.
