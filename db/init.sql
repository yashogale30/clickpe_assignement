-- Loan Eligibility Engine schema. This file is run by Postgres on first volume initialization.

-- n8n stores its own metadata separately from application data.
CREATE DATABASE n8n;

-- Tracks every uploaded CSV and its processing funnel.
CREATE TABLE batches (
    batch_id UUID PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('uploaded', 'parsed', 'matching', 'matched', 'notified', 'failed')),
    total_users INTEGER NOT NULL DEFAULT 0,
    valid_users INTEGER NOT NULL DEFAULT 0,
    rejected_users INTEGER NOT NULL DEFAULT 0,
    after_range_join INTEGER NOT NULL DEFAULT 0,
    matched_count INTEGER NOT NULL DEFAULT 0,
    near_miss_count INTEGER NOT NULL DEFAULT 0,
    emailed_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Canonical applicant records, populated from the staging table after validation.
CREATE TABLE users (
    user_id TEXT PRIMARY KEY,
    email TEXT NOT NULL,
    monthly_income NUMERIC NOT NULL,
    credit_score INTEGER NOT NULL,
    employment_status TEXT NOT NULL,
    age INTEGER NOT NULL,
    batch_id UUID NOT NULL REFERENCES batches(batch_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Constraint-free COPY destination for rows streamed from an uploaded CSV.
CREATE TABLE staging_users (
    user_id TEXT,
    email TEXT,
    monthly_income NUMERIC,
    credit_score INTEGER,
    employment_status TEXT,
    age INTEGER,
    batch_id UUID,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

-- Loan criteria collected by the crawler. Employment is stored as an allowed set.
CREATE TABLE loan_products (
    product_id SERIAL PRIMARY KEY,
    source_site TEXT NOT NULL,
    product_name TEXT NOT NULL,
    interest_rate NUMERIC NOT NULL,
    min_income NUMERIC,
    min_score INTEGER,
    max_score INTEGER,
    min_age INTEGER,
    max_age INTEGER,
    allowed_employment TEXT[],
    qualitative_notes TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    last_seen_at TIMESTAMPTZ,
    UNIQUE (source_site, product_name)
);

-- Applicant/product eligibility outcomes. A negative margin represents a near miss.
CREATE TABLE matches (
    id BIGSERIAL PRIMARY KEY,
    user_id TEXT NOT NULL REFERENCES users(user_id),
    product_id INTEGER NOT NULL REFERENCES loan_products(product_id),
    margin NUMERIC NOT NULL,
    reason TEXT,
    is_near_miss BOOLEAN NOT NULL DEFAULT FALSE,
    notified BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, product_id)
);

-- Input rows rejected during validation, retained with a machine-readable original payload.
CREATE TABLE rejected_rows (
    id BIGSERIAL PRIMARY KEY,
    batch_id UUID NOT NULL REFERENCES batches(batch_id),
    raw_row JSONB NOT NULL,
    reason TEXT NOT NULL
);

-- One record per crawl execution, including whether the LLM fallback was required.
CREATE TABLE crawl_runs (
    id BIGSERIAL PRIMARY KEY,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    status TEXT NOT NULL CHECK (status IN ('ok', 'degraded', 'failed')),
    used_llm BOOLEAN NOT NULL DEFAULT FALSE,
    notes TEXT
);

-- Cached extracted page data so unchanged sources do not invoke the LLM again.
CREATE TABLE page_cache (
    url TEXT PRIMARY KEY,
    content_hash TEXT NOT NULL,
    last_checked_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_extracted JSONB
);

-- Supports the numeric range join that powers eligibility matching.
CREATE INDEX idx_users_credit_score ON users (credit_score);
CREATE INDEX idx_users_monthly_income ON users (monthly_income);
CREATE INDEX idx_users_age ON users (age);
CREATE INDEX idx_matches_notified ON matches (notified);

-- Keep application-managed timestamps current on updates.
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER batches_set_updated_at
BEFORE UPDATE ON batches
FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TRIGGER users_set_updated_at
BEFORE UPDATE ON users
FOR EACH ROW EXECUTE FUNCTION set_updated_at();
