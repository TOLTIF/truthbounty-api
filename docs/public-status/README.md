# Public Service-Status Projection (UX-BE-001)

`GET /status/public` returns a sanitized, versioned (`public-status.v1`) status
projection for the public status page. It is unauthenticated, cached and rate
limited. The protected `/health/*` reports are unchanged and are **not** the
source of truth for the public page.

## Published artifacts

| File | Purpose |
| --- | --- |
| [`openapi.json`](./openapi.json) | OpenAPI 3 document for the endpoint and its DTOs |
| [`fixtures.json`](./fixtures.json) | Frontend fixtures, one per scenario (operational, unavailable, delayed, stale, wrong release, not configured) |

Both are verified against the code by
`src/health/public-status/public-status.artifacts.spec.ts`. Regenerate after a
schema change with:

```bash
UPDATE_PUBLIC_STATUS_ARTIFACTS=1 npx jest public-status.artifacts
```

## Schema

- `components[]`: `web`, `api`, `rpc`, `indexer`, `database`, `evidence_gateway`
- `release`: release ID, chain ID, deployment block, artifact checksum
- `chain`: finalized block, indexed block, lag in blocks
- `cache`: `maxAgeSeconds`, `staleAfterSeconds`, `expiresAt`

Every component, `release` and `chain` carries an `observation` with `source`,
`observedAt`, `ageSeconds` and `freshness` (`fresh` | `stale` | `unknown`).

States: `operational`, `degraded`, `delayed`, `stale`, `unavailable`, `unknown`.
`overall` is the worst state, in the order
operational < degraded < delayed < stale < unknown < unavailable.

## Fail-closed rules

- No observation → `unknown` (source `not_observed`). Never `operational`.
- Observation older than `staleAfterSeconds` → `stale`.
- Any failed dependency probe → `unavailable`, whatever its internal criticality.
- Finalized lag beyond policy → `delayed`.
- Cursor bound to a different release → `unavailable`, and no block numbers are published.

## Redaction

The response is built field-by-field from allow-listed DTOs
(`src/health/public-status/public-status.dto.ts`); nothing from internal
reports is spread or passed through. Hostnames, URLs, failure messages, stack
traces, response times, criticality, diagnostics and secrets never appear.

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `PUBLIC_STATUS_CACHE_TTL_SECONDS` | `15` | Server cache and `Cache-Control: max-age` |
| `PUBLIC_STATUS_STALE_AFTER_SECONDS` | `120` | Age after which observations are `stale` |
| `PUBLIC_STATUS_WEB_HEALTH_URL` | unset | Web frontend URL to probe; `web` is `unknown` when unset |
| `PUBLIC_STATUS_WEB_PROBE_TIMEOUT_MS` | `2000` | Web probe timeout |
| `PUBLIC_STATUS_RATE_LIMIT` | `60` | Requests per client IP per window |
| `PUBLIC_STATUS_RATE_WINDOW_SECONDS` | `60` | Rate limit window |

Chain, indexer, RPC and release data come from the finalized-block cursor
(PREWAVE-BE-001, see [`../FINALIZED_CURSOR.md`](../FINALIZED_CURSOR.md)); they
are `unknown` until a release binding is configured.
