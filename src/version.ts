// Re-export the published package name + version from package.json so
// `telemetry.sdk.version` and the OTLP user-agent always match the real
// published artifact. Previously these values were read from a generated
// `versions.json` that had to be rebuilt before every publish — if the
// build step was skipped (e.g. `cs-publish` invoked directly instead of
// `run-s release`), the published tarball shipped a stale version string
// while the code behaved as the new version. Importing package.json
// directly relies on tsup's `resolveJsonModule` + bundling to inline the
// value at build time, so there's nothing to keep in sync.
// Import-only — the value is inlined by tsup at build time via
// resolveJsonModule. `import * as` avoids requiring esModuleInterop.
import * as pkg from '../package.json'

export const PKG_NAME: string = (pkg as { name: string }).name
export const PKG_VERSION: string = (pkg as { version: string }).version
