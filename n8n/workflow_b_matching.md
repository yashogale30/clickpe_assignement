# Workflow B: loan matching

This document is the implementation artifact for the n8n matching workflow. It
uses the application PostgreSQL database, not n8n's internal database. No AWS
deployment, email delivery, or real credential is included.

## Schema prerequisites

The inspected schema in [`db/init.sql`](../db/init.sql) has these relevant
tables and columns:

- `batches(batch_id UUID, status, total_users, valid_users, rejected_users,
  after_range_join, matched_count, near_miss_count, emailed_count)`
- `users(user_id, email, monthly_income, credit_score, employment_status, age,
  batch_id)`
- `loan_products(product_id, min_income, min_score, max_score, min_age,
  max_age, allowed_employment, is_active)`
- `matches(user_id, product_id, margin, reason, is_near_miss, notified)` with
  `UNIQUE (user_id, product_id)`

Two small schema changes are required by the requested behavior:

1. A batch must be able to remain in `matching` while Workflow B runs.
2. NULL product thresholds and NULL `allowed_employment` must be representable.

The checked-in init script now includes those changes. Existing Docker volumes
do not rerun init scripts, so apply this one-time migration to an already
initialized database before activating Workflow B:

```sql
ALTER TABLE batches DROP CONSTRAINT IF EXISTS batches_status_check;
ALTER TABLE batches ADD CONSTRAINT batches_status_check
  CHECK (status IN ('uploaded', 'parsed', 'matching', 'matched', 'notified', 'failed'));

ALTER TABLE loan_products
  ALTER COLUMN min_income DROP NOT NULL,
  ALTER COLUMN min_score DROP NOT NULL,
  ALTER COLUMN max_score DROP NOT NULL,
  ALTER COLUMN min_age DROP NOT NULL,
  ALTER COLUMN max_age DROP NOT NULL,
  ALTER COLUMN allowed_employment DROP NOT NULL;
```

Run it locally without changing volumes:

```sh
docker compose exec -T postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<'SQL'
ALTER TABLE batches DROP CONSTRAINT IF EXISTS batches_status_check;
ALTER TABLE batches ADD CONSTRAINT batches_status_check
  CHECK (status IN ('uploaded', 'parsed', 'matching', 'matched', 'notified', 'failed'));
ALTER TABLE loan_products
  ALTER COLUMN min_income DROP NOT NULL,
  ALTER COLUMN min_score DROP NOT NULL,
  ALTER COLUMN max_score DROP NOT NULL,
  ALTER COLUMN min_age DROP NOT NULL,
  ALTER COLUMN max_age DROP NOT NULL,
  ALTER COLUMN allowed_employment DROP NOT NULL;
SQL
```

The SQL below is NULL-safe even though the seed products currently have all
criteria populated. A NULL numeric threshold means that criterion passes and
contributes `1` to the normalized margin. A NULL employment array means any
employment status is allowed.

## n8n credentials and shared-secret setup

Create a PostgreSQL credential named `Loan Eligibility Postgres`:

- Host: `postgres` when n8n runs in the supplied Docker Compose network;
  `localhost` is not correct from inside the n8n container.
- Port: `5432` (the container port, not host port `5433`)
- Database: `loan_eligibility`
- User/password: values from the local `.env`; enter them in n8n Credentials,
  never in a workflow node or this repository.
- SSL: match the database deployment; leave disabled for the local Compose
  network.

Create a Header Auth credential named `Loan Match Webhook Auth`:

- Header name: `Authorization`
- Header value: a long random secret, stored only in n8n and in the caller's
  secret manager/environment.

Do not put the secret in a URL, query string, exported workflow, curl history,
or committed file. In production use HTTPS, restrict who can edit credentials,
rotate the secret, and configure the ingestion caller to send the exact same
`Authorization` header. n8n stores credential values encrypted using
`N8N_ENCRYPTION_KEY`; set that to a real, persistent secret in the n8n
environment rather than the placeholder in `.env`.

## Exact node-by-node UI configuration

Create a new workflow named **Workflow B - Matching**. Use the following
nodes and connections in order.

### 1. Webhook — `Loan match webhook`

In **Webhook**:

- HTTP Method: `POST`
- Path: `loan_match`
- Authentication: `Header Auth`
- Credential: `Loan Match Webhook Auth`
- Respond: `Using "Respond to Webhook" Node`
- Options → Allowed Origins: leave unset unless the caller requires a
  specific origin

After saving and activating, the production endpoint is:

```text
http://localhost:5678/webhook/loan_match
```

For a deployed HTTPS n8n instance it is:

```text
https://YOUR_N8N_HOST/webhook/loan_match
```

The editor/test URL contains `/webhook-test/` and is not the requested
production URL. The expected JSON body is
`{"batch_id":"8685d1b6-631f-4513-b3d8-5b904854cdb1"}`. An optional
`tolerance` number overrides the default `0.05`.

### 2. Set — `Normalize request`

Add a **Set** node. Keep only the following fields:

| Field | Type | Value |
|---|---|---|
| `batch_id` | String | `={{$json.body.batch_id}}` |
| `tolerance` | Number | `={{$json.body.tolerance ?? 0.05}}` |

Add a **Postgres** node after it, with the `Loan Eligibility Postgres`
credential, operation **Execute Query**, named **Mark batch matching**.
Enable **Continue On Fail** only if the node's error output is connected to
`Mark batch failed`; otherwise use the node's **On Error → Continue (using
error output)** setting.

Query:

```sql
UPDATE batches
SET status = 'matching'
WHERE batch_id = $1
  AND status IN ('parsed', 'matching')
RETURNING batch_id, status;
```

Query Parameters: `={{[$node["Normalize request"].json.batch_id]}}`.
Add an **IF** node named `Batch was marked matching` after this node:

- Condition type: `Boolean`
- Value 1: `={{!!$json.batch_id}}`
- Operation: `is true`

Connect the true output to `Match batch users`. Connect the false output to
`Mark batch failed`. The returned row is therefore an explicit audit guard:
a missing batch cannot proceed as a successful match.

### 3. Postgres — `Match batch users`

Use the same credential, **Execute Query**, and connect the success output from
`Mark batch matching`. Set **On Error → Continue (using error output)** and
connect its error output to `Mark batch failed`.

Query Parameters:
`={{[$node["Normalize request"].json.batch_id, $node["Normalize request"].json.tolerance]}}`.

The query performs the numeric range join, exact employment check, normalized
margin calculation, one-criterion near-miss selection, per-user ranking, and
idempotent insert:

```sql
WITH
input AS (
  SELECT $1::uuid AS batch_id, GREATEST(0::numeric, $2::numeric) AS tolerance
),
candidate_pairs AS (
  SELECT
    u.user_id, u.email, u.monthly_income, u.credit_score,
    u.employment_status, u.age, u.batch_id,
    p.product_id, p.product_name, p.source_site,
    p.min_income, p.min_score, p.max_score, p.min_age, p.max_age,
    p.allowed_employment, i.tolerance
  FROM users u
  JOIN loan_products p ON p.is_active
  CROSS JOIN input i
  WHERE u.batch_id = i.batch_id
),
scored AS (
  SELECT c.*,
    CASE WHEN c.min_income IS NULL THEN TRUE
         ELSE c.monthly_income >= c.min_income END AS income_ok,
    CASE WHEN c.min_score IS NULL THEN TRUE
         ELSE c.credit_score >= c.min_score END AS min_score_ok,
    CASE WHEN c.max_score IS NULL THEN TRUE
         ELSE c.credit_score <= c.max_score END AS max_score_ok,
    CASE WHEN c.min_age IS NULL THEN TRUE
         ELSE c.age >= c.min_age END AS min_age_ok,
    CASE WHEN c.max_age IS NULL THEN TRUE
         ELSE c.age <= c.max_age END AS max_age_ok,
    CASE WHEN c.allowed_employment IS NULL THEN TRUE
         ELSE c.employment_status = ANY(c.allowed_employment) END AS employment_ok,
    LEAST(
      COALESCE((c.monthly_income - c.min_income) /
        NULLIF(GREATEST(ABS(c.min_income), 1), 0), 1),
      COALESCE((c.credit_score - c.min_score)::numeric /
        NULLIF(GREATEST(ABS(c.min_score), 1), 0), 1),
      COALESCE((c.max_score - c.credit_score)::numeric /
        NULLIF(GREATEST(ABS(c.max_score), 1), 0), 1),
      COALESCE((c.age - c.min_age)::numeric /
        NULLIF(GREATEST(ABS(c.min_age), 1), 0), 1),
      COALESCE((c.max_age - c.age)::numeric /
        NULLIF(GREATEST(ABS(c.max_age), 1), 0), 1)
    ) AS normalized_margin
  FROM candidate_pairs c
),
classified AS (
  SELECT s.*,
    (s.income_ok AND s.min_score_ok AND s.max_score_ok AND
     s.min_age_ok AND s.max_age_ok AND s.employment_ok) AS is_match,
    (NOT s.income_ok)::int + (NOT s.min_score_ok)::int +
    (NOT s.max_score_ok)::int + (NOT s.min_age_ok)::int +
    (NOT s.max_age_ok)::int + (NOT s.employment_ok)::int AS failed_criteria,
    (CASE WHEN s.income_ok THEN 0 ELSE
       GREATEST(s.min_income - s.monthly_income, 0) /
       GREATEST(ABS(COALESCE(s.min_income, 1)), 1) END) AS income_deficit,
    (CASE WHEN s.min_score_ok THEN 0 ELSE
       GREATEST(s.min_score - s.credit_score, 0)::numeric /
       GREATEST(ABS(COALESCE(s.min_score, 1)), 1) END) AS min_score_deficit,
    (CASE WHEN s.max_score_ok THEN 0 ELSE
       GREATEST(s.credit_score - s.max_score, 0)::numeric /
       GREATEST(ABS(COALESCE(s.max_score, 1)), 1) END) AS max_score_deficit,
    (CASE WHEN s.min_age_ok THEN 0 ELSE
       GREATEST(s.min_age - s.age, 0)::numeric /
       GREATEST(ABS(COALESCE(s.min_age, 1)), 1) END) AS min_age_deficit,
    (CASE WHEN s.max_age_ok THEN 0 ELSE
       GREATEST(s.age - s.max_age, 0)::numeric /
       GREATEST(ABS(COALESCE(s.max_age, 1)), 1) END) AS max_age_deficit
  FROM scored s
),
eligible AS (
  SELECT c.*, FALSE AS is_near_miss,
    'all criteria satisfied'::text AS reason
  FROM classified c
  WHERE c.is_match
),
near_misses AS (
  SELECT c.*, TRUE AS is_near_miss,
    CASE
      WHEN NOT c.income_ok THEN 'failed criterion: minimum income'
      WHEN NOT c.min_score_ok THEN 'failed criterion: minimum credit score'
      WHEN NOT c.max_score_ok THEN 'failed criterion: maximum credit score'
      WHEN NOT c.min_age_ok THEN 'failed criterion: minimum age'
      WHEN NOT c.max_age_ok THEN 'failed criterion: maximum age'
    END::text AS reason
  FROM classified c
  WHERE NOT c.is_match
    AND c.employment_ok
    AND c.failed_criteria = 1
    AND (
      (NOT c.income_ok AND c.income_deficit <= c.tolerance)
      OR (NOT c.min_score_ok AND c.min_score_deficit <= c.tolerance)
      OR (NOT c.max_score_ok AND c.max_score_deficit <= c.tolerance)
      OR (NOT c.min_age_ok AND c.min_age_deficit <= c.tolerance)
      OR (NOT c.max_age_ok AND c.max_age_deficit <= c.tolerance)
    )
),
ranked AS (
  SELECT x.*,
    ROW_NUMBER() OVER (
      PARTITION BY x.user_id, x.is_near_miss
      ORDER BY x.is_near_miss, x.normalized_margin DESC, x.product_id
    ) AS product_rank
  FROM (
    SELECT * FROM eligible
    UNION ALL
    SELECT * FROM near_misses
  ) x
),
limited AS (
  SELECT * FROM ranked
  WHERE (NOT is_near_miss AND product_rank <= 3)
     OR (is_near_miss AND product_rank <= 1)
),
inserted AS (
  INSERT INTO matches (user_id, product_id, margin, reason, is_near_miss)
  SELECT user_id, product_id, normalized_margin, reason, is_near_miss
  FROM limited
  ON CONFLICT (user_id, product_id) DO NOTHING
  RETURNING user_id, product_id, is_near_miss
),
funnel AS (
  SELECT
    (SELECT COUNT(*) FROM classified
      WHERE income_ok AND min_score_ok AND max_score_ok
        AND min_age_ok AND max_age_ok) AS range_join_count,
    (SELECT COUNT(*) FROM inserted WHERE NOT is_near_miss) AS inserted_matches,
    (SELECT COUNT(*) FROM inserted WHERE is_near_miss) AS inserted_near_misses
)
UPDATE batches b
SET after_range_join = f.range_join_count,
    matched_count = f.inserted_matches,
    near_miss_count = f.inserted_near_misses,
    status = 'matched'
FROM funnel f
WHERE b.batch_id = $1
RETURNING b.batch_id, b.status, b.after_range_join,
          b.matched_count, b.near_miss_count;
```

`ON CONFLICT DO NOTHING` deliberately preserves an existing row's
`notified` value and all other notification state. The counters report rows
inserted by this execution; re-running the same batch is therefore
idempotent and does not claim that old rows were newly inserted.

### 4. Execute Workflow — `Invoke Workflow C (not yet available)`

Connect the success output from `Match batch users` to an **Execute Workflow**
node. Select **Workflow C** when it is created. Pass these fields:

```json
{
  "batch_id": "={{$node['Normalize request'].json.batch_id}}"
}
```

Workflow C does **not exist yet** in this repository. Leave this node disabled
or set it to **Wait for Sub-Workflow Completion** only after Workflow C has
been implemented and selected. Do not test email delivery as part of this
artifact.

### 5. Respond to Webhook — `Matching accepted`

Add **Respond to Webhook** after the Execute Workflow node:

- Respond With: `JSON`
- Response Code: `200`
- Response Body:

```json
{
  "batch_id": "={{$node['Normalize request'].json.batch_id}}",
  "status": "matched",
  "message": "Matching completed; Workflow C is not implemented yet."
}
```

### 6. Error path — `Mark batch failed`

For `Mark batch matching`, `Match batch users`, and the Execute Workflow node,
set **On Error → Continue (using error output)** and connect each error output
to this Postgres node. Also connect an invalid/missing-batch guard to it if
you add an IF node after `Mark batch matching`.

Use the same PostgreSQL credential and **Execute Query**:

```sql
UPDATE batches
SET status = 'failed'
WHERE batch_id = $1
RETURNING batch_id, status;
```

Query Parameters:
`={{[$node["Normalize request"].json.batch_id]}}`.

Connect it to a **Respond to Webhook** node named `Matching failed`:

- Respond With: `JSON`
- Response Code: `500`
- Response Body:

```json
{
  "batch_id": "={{$node['Normalize request'].json.batch_id}}",
  "status": "failed",
  "message": "Workflow B failed; inspect the n8n execution."
}
```

Do not use a success-shaped fallback on this path. If the failure node itself
cannot connect to PostgreSQL, n8n's execution error remains visible in the
execution log and the batch may retain its prior status; alert on failed n8n
executions.

## Test queries

Run these read-only checks after a test execution:

```sql
SELECT batch_id, status, total_users, valid_users, rejected_users,
       after_range_join, matched_count, near_miss_count, emailed_count
FROM batches
WHERE batch_id = '8685d1b6-631f-4513-b3d8-5b904854cdb1';

SELECT m.user_id, m.product_id, m.margin, m.is_near_miss, m.notified,
       p.product_name
FROM matches m
JOIN users u USING (user_id)
JOIN loan_products p USING (product_id)
WHERE u.batch_id = '8685d1b6-631f-4513-b3d8-5b904854cdb1'
ORDER BY m.user_id, m.is_near_miss, m.margin DESC, m.product_id;

SELECT user_id, COUNT(*) FILTER (WHERE NOT is_near_miss) AS match_count,
       COUNT(*) FILTER (WHERE is_near_miss) AS near_miss_count
FROM matches
WHERE user_id IN (
  SELECT user_id FROM users
  WHERE batch_id = '8685d1b6-631f-4513-b3d8-5b904854cdb1'
)
GROUP BY user_id;
```

Expected results for the supplied ingestion batch are **3 total users, 2
valid users, and 1 rejected user**. With the six active seed products in
[`db/seed_products.sql`](../db/seed_products.sql), the rollback-only
validation against the current data produced **8** numeric range-join pairs,
**6** regular matches, and **1** near miss. After a real execution,
`status` should be `matched` and those three counters should be
`8`, `6`, and `1`, respectively. If Workflow A has changed the active
product set, the exact counts will differ; the per-user query must still show
no more than three regular matches and one near miss for this execution.

## Curl commands

Use a placeholder secret locally; do not copy a real secret into shell history:

```sh
export LOAN_MATCH_SECRET='use-the-same-secret-configured-in-n8n'
curl --fail-with-body -X POST \
  'http://localhost:5678/webhook/loan_match' \
  -H 'Content-Type: application/json' \
  -H "Authorization: ${LOAN_MATCH_SECRET}" \
  --data '{"batch_id":"8685d1b6-631f-4513-b3d8-5b904854cdb1"}'
```

To exercise the tolerance override:

```sh
curl --fail-with-body -X POST \
  'http://localhost:5678/webhook/loan_match' \
  -H 'Content-Type: application/json' \
  -H "Authorization: ${LOAN_MATCH_SECRET}" \
  --data '{"batch_id":"8685d1b6-631f-4513-b3d8-5b904854cdb1","tolerance":0.05}'
```

For the host-side database port, inspect results with:

```sh
docker compose exec postgres psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" \
  -c "SELECT batch_id, status, after_range_join, matched_count, near_miss_count FROM batches WHERE batch_id = '8685d1b6-631f-4513-b3d8-5b904854cdb1';"
```

No `workflow_b_matching.json` is included. n8n export JSON contains
instance-specific credential references and version-dependent node metadata;
without a running n8n instance to export and verify, fabricating an import
file would be unreliable. Build the nodes above in the UI, then use
**Workflow → Download** to create a native export if desired.
