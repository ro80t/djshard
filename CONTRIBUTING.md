# Contributing to djshard

Thanks for helping out. Bug reports and pull requests are welcome at [ro80t/djshard](https://github.com/ro80t/djshard).

## Requirements

- [Bun](https://bun.sh) 1.2 or later
- [Go](https://go.dev) 1.26 or later (only for the operator)

## Setup

```sh
bun install
bun run skills   # reinstall the agent skills pinned in skills-lock.json (optional; they are committed)
```

## Layout

| Path             | What                                                                                |
| ---------------- | ----------------------------------------------------------------------------------- |
| `src/shared.ts`  | Wire protocol (`Msg`), request tracking, and shard assignment math.                 |
| `src/manager.ts` | `ClusterManager`: spawns clusters and talks to a bridge.                            |
| `src/client.ts`  | `ClusterClient` and `getInfo()`, used inside a cluster process.                     |
| `src/bridge.ts`  | `Bridge` for cross-host setups without Kubernetes.                                  |
| `test/`          | Integration tests that spawn real cluster processes and a bridge.                   |
| `operator/`      | Kubernetes operator in Go: controller, built-in bridge, and manifests in `config/`. |

The TypeScript bridge and the Go bridge speak the same JSON protocol. If you change `Msg` in `src/shared.ts` or `assignment()`, change `operator/bridge.go` too. The assignment test cases are mirrored in `test/djshard.test.ts` and `operator/bridge_test.go`.

## Checks

Everything CI runs:

```sh
bun run ci          # oxlint, oxfmt --check, bun check (types), bun test, tsdown build
cd operator && go vet ./... && go test ./...
```

Individual commands:

| Command             | Does                                     |
| ------------------- | ---------------------------------------- |
| `bun run lint`      | Lint with oxlint.                        |
| `bun run fmt`       | Format with oxfmt.                       |
| `bun run typecheck` | Type-check with `bun check`.             |
| `bun test`          | Run the tests.                           |
| `bun run build`     | Build ESM, CJS and `.d.ts` into `dist/`. |

Go code is formatted with `gofmt`.

## Pull requests

- Branch from `main`. Don't push to `main` directly.
- Keep each change focused, and add a test when you change behavior.
- Make sure `bun run ci` and `go test ./...` pass.
- Add a changeset (`bunx changeset`) when the change affects users of the library or the operator.
- Describe what changed, why, and how you verified it.

## Releases

Releases use [changesets](https://github.com/changesets/changesets). The library and the operator image share one version.

1. Each user-facing PR adds a changeset with `bunx changeset`.
2. When changesets land on `main`, the Release workflow opens or updates a `chore: release` PR that bumps the version and the changelog.
3. Merging that PR publishes the package to npm with trusted publishing, tags the commit `vX.Y.Z` with a GitHub release, and pushes `ghcr.io/ro80t/djshard-operator:<version>` and `:latest`.

Publishing needs no npm token: npmjs.com lists `ro80t/djshard`'s `release.yml` as the trusted publisher for `djshard`.

## Automation

| Workflow / config          | Does                                                                                                     |
| -------------------------- | -------------------------------------------------------------------------------------------------------- |
| `.github/workflows/ci.yml` | Library checks (lint, format, types, tests, build) and operator checks (gofmt, vet, tests, image build). |
| `release.yml`              | Changesets release PR, npm publish, operator image push.                                                 |
| `skills-update.yml`        | Weekly `bunx skills update`; opens a PR when a pinned skill changed upstream.                            |
| `.github/dependabot.yml`   | Bun, Go modules (Kubernetes deps grouped), the operator's Docker base images, and GitHub Actions.        |

## Agent skills

The repo pins skills for coding agents (Claude Code, Codex, ...) with the [skills CLI](https://github.com/vercel-labs/skills). Both `skills-lock.json` and the installed copies in `.claude/skills` and `.agents/skills` are committed, so the skills work right after cloning.

| Source                                                        | Skills                                                            |
| ------------------------------------------------------------- | ----------------------------------------------------------------- |
| [ro80t/hush](https://github.com/ro80t/hush)                   | `hush`, `hush-review`, `hush-fix`: comment and naming style.      |
| [ro80t/github-skills](https://github.com/ro80t/github-skills) | `git-policy`, `open-pr`, `auto-commit-and-push`, `fix-coderabbit` |

```sh
bun run skills                          # install what skills-lock.json pins
bunx skills add <owner/repo> --skill "*" --copy # add a skill source (updates skills-lock.json)
bunx skills update -p                   # update project skills
bunx skills remove <name>               # remove one
```

Commit `skills-lock.json` whenever you change it.
