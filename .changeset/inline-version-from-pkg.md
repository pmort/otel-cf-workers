---
"@pmort/otel-cf-workers": patch
---

Read the SDK name + version from `package.json` directly instead of a generated `versions.json`. The previous setup only regenerated `versions.json` if the full `release` script ran; publishing through any shortcut (e.g. `cs-publish` directly) shipped a tarball with a stale `telemetry.sdk.version` string — the code behaved as the new version but reported as the old one. Now the value is inlined by tsup at bundle time from the actual `package.json`, so it cannot drift. Also adds a `prepack` hook that forces a clean rebuild on publish as belt-and-braces. The `telemetry.sdk.build.node_version` resource attribute is dropped — it was a build-time snapshot that isn't useful to workerd consumers.
