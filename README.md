# ciwatch

One live view of CI jobs across GitHub Actions, CircleCI and self-hosted
runner pools: what is queued, what is running, and where.

If your CI is spread across providers, say GitHub-hosted runners, a pool of
self-hosted or ephemeral runners, and CircleCI as overflow, each one shows
only its own jobs. ciwatch puts them on one page and in one set of
Prometheus metrics.

- **GitHub Actions** jobs arrive live through signed `workflow_job` webhooks.
  An optional poll fills the view at start-up and repairs missed deliveries.
- **CircleCI** jobs are polled from API v2. CircleCI only sends webhooks when
  work finishes, so polling is the only way to see queued and running jobs.
- **Pools** group jobs by where they run, using rules on runner labels, runner
  names, runner groups, repositories and job names.

ciwatch is a single binary that holds no database: jobs live in memory for
a retention window (24 hours by default).

## Quick start

```sh
cargo install --git https://github.com/syz51/ciwatch
cp ciwatch.example.toml ciwatch.toml   # then edit it
ciwatch --config ciwatch.toml --check  # validate
ciwatch --config ciwatch.toml
```

Open `http://127.0.0.1:8080/`.

For GitHub webhooks, add a repository or organization webhook (or a GitHub
App) that points at `https://YOUR-HOST/webhooks/github`. Set content type
`application/json`, use the configured secret, and subscribe to **Workflow
jobs** only.

## Configuration

See [`ciwatch.example.toml`](ciwatch.example.toml). Secrets are referenced
by environment variable or file, never written into the config. Unknown
keys are rejected.

| Section | Key | Default | Meaning |
| --- | --- | --- | --- |
| top level | `listen` | `127.0.0.1:8080` | HTTP listen address |
| | `retention_hours` | `24` | Drop jobs not updated for this long (1 to 8784) |
| `[github]` | `webhook_secret` | none | Turns on `POST /webhooks/github` |
| | `webhook_listen` | none | Serve the webhook only on this address, and not on `listen` |
| | `token`, `repositories` | none | Poll these `owner/name` repositories; the token needs Actions read access |
| | `poll_interval_secs` | `60` | At least 10 |
| | `api_url` | `https://api.github.com` | GitHub Enterprise: `https://HOST/api/v3` |
| `[circleci]` | `token`, `projects` | required | API token and `gh/org/repo`-style slugs |
| | `poll_interval_secs` | `30` | At least 5 |
| | `lookback_hours` | `6` | Pipelines created within this window are polled; workflows still held as active are refreshed after it |
| `[[pools]]` | `name` plus conditions | none | First matching rule wins |

A pool rule matches when every condition it sets matches:

- `provider`: `github` or `circleci`
- `repository`: an exact `owner/name`
- `labels_any`: the job asks for any of these labels
- `runner_prefix`: the assigned runner's name starts with this
- `runner_group`: the exact runner group name
- `job_prefix`: the job name starts with this

Jobs that match no rule fall into these defaults:

- GitHub: `github-hosted` (runner group "GitHub Actions"), `self-hosted` (any
  other runner), or `unassigned` (queued with no runner yet)
- CircleCI: `circleci`

Label rules classify queued jobs; runner rules apply once a runner picks a
job up.

## Endpoints

| Path | What |
| --- | --- |
| `/` | The page; it refreshes every 5 seconds |
| `/api/jobs` | JSON: `jobs`, plus `sources` with each source's last success and last error |
| `/metrics` | Prometheus text format |
| `/healthz` | `ok` |
| `POST /webhooks/github` | GitHub webhook receiver, when `webhook_secret` is set; on `webhook_listen` instead when that is set |

### Metrics

| Metric | Labels |
| --- | --- |
| `ciwatch_jobs` (gauge) | `provider`, `pool`, `repository`, `state` |
| `ciwatch_oldest_queued_job_age_seconds` (gauge) | `provider`, `pool` |
| `ciwatch_source_last_success_timestamp_seconds` (gauge) | `source` |
| `ciwatch_source_errors_total` (counter) | `source` |
| `ciwatch_webhook_deliveries_total` (counter) | `result`: `accepted`, `ignored`, `rejected` |

States are `queued`, `waiting` (approval, environment protection or a blocked
upstream job), `running`, `success`, `failure`, `cancelled` and `skipped`.
Each job also keeps the provider's own status word.

### Job model

Field names map to the OpenTelemetry CI/CD and VCS semantic conventions:

| ciwatch | OpenTelemetry attribute |
| --- | --- |
| `pipeline` | `cicd.pipeline.name` |
| `run_id` | `cicd.pipeline.run.id` |
| `name` | `cicd.pipeline.task.name` |
| `id` | `cicd.pipeline.task.run.id` |
| `url` | `cicd.pipeline.task.run.url.full` |
| `runner` | `cicd.worker.name` |
| `repository` | `vcs.repository.name` |
| `branch` | `vcs.ref.head.name` |
| `revision` | `vcs.ref.head.revision` |

## Container image

Each `v*` tag publishes `ghcr.io/syz51/ciwatch:<tag>` for linux/amd64, with
a build provenance attestation. The image runs as an unprivileged user and
reads `/etc/ciwatch/ciwatch.toml`:

```sh
docker run --read-only -p 127.0.0.1:8080:8080 \
  -e CIWATCH_GITHUB_WEBHOOK_SECRET -e CIWATCH_GITHUB_TOKEN \
  -v "$PWD/ciwatch.toml:/etc/ciwatch/ciwatch.toml:ro" \
  -v "$PWD/circleci-token:/etc/ciwatch/circleci-token:ro" \
  ghcr.io/syz51/ciwatch:v0.1.1
```

Set `listen = "0.0.0.0:8080"` in that file so the published port reaches it.
The example passes the secrets that `ciwatch.example.toml` names, since a
secret file resolves next to the config file. With `webhook_listen`, also
publish its port.
Verify an image with `gh attestation verify oci://ghcr.io/syz51/ciwatch:<tag> -R syz51/ciwatch`.

## Security

- The page, `/api/jobs` and `/metrics` have no authentication. Bind to
  localhost or a private network, or put them behind an authenticating
  proxy. They show repository names, branches, job names and runner names.
- Webhook deliveries are checked against `X-Hub-Signature-256`, and
  unsigned or mis-signed deliveries are refused.
- To receive webhooks from the internet without exposing the page, set
  `webhook_listen` and route only that address to the internet. It serves
  `POST /webhooks/github` and nothing else.
- Each listener closes a connection that has not sent a request's headers
  within 10 seconds, answers 408 to a request that takes over 30 seconds,
  accepts at most 256 connections at once, and drops connections still open
  10 seconds after shutdown starts. It speaks HTTP/1 only. A reverse proxy in
  front can enforce tighter limits per client.
- Give ciwatch read-only credentials. It never triggers, cancels or reruns
  anything.
- The page is served with `Content-Security-Policy: default-src 'self'`.
  Job links are only rendered when they are `http` or `https` URLs.

## Limits

- State is in memory. After a restart, webhook-only setups show jobs only
  from the next delivery, while polling refills queued and running jobs.
- GitHub polling lists runs that are requested, pending, queued, waiting or
  in progress, plus every run ciwatch still holds as active, and reads all
  attempts of each run. It reads at most 100 runs per status and 100 jobs per
  run. A run that returns 404 is dropped.
- CircleCI's API gives no per-job queue time, so a CircleCI job's
  `created_at` is when ciwatch first saw it. Jobs a finished workflow left
  blocked or on hold are shown as `skipped`.
- CircleCI's API does not say which executor or runner ran a job, so
  CircleCI pools come from repository and job-name rules.
- Each CircleCI poll makes one request per recent pipeline plus one per
  unfinished workflow, and two per active workflow older than the lookback. Size `lookback_hours` and `poll_interval_secs` for
  your volume and rate limits.

## Development

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
```

The tests drive the webhook through the HTTP router, and run both pollers
against local mock APIs that serve responses in the providers' documented
shapes.

## License

MIT
