-- Demo products used when crawler data is unavailable. Rates and criteria are illustrative.
INSERT INTO loan_products (
    source_site, product_name, interest_rate, min_income, min_score, max_score,
    min_age, max_age, allowed_employment, qualitative_notes, last_seen_at
) VALUES
    ('demo-finance.example', 'Starter Personal Loan', 14.50, 20000, 650, 900, 21, 55,
     ARRAY['salaried', 'self_employed'], 'Entry-level unsecured personal loan.', NOW()),
    ('demo-finance.example', 'Salaried Flex Loan', 11.25, 35000, 700, 900, 21, 60,
     ARRAY['salaried'], 'For salaried applicants with stable income.', NOW()),
    ('demo-finance.example', 'Professional Advantage Loan', 10.75, 60000, 750, 900, 25, 60,
     ARRAY['salaried', 'self_employed'], 'Preferential rate for strong profiles.', NOW()),
    ('trusted-credit.example', 'Self-Employed Growth Loan', 13.25, 45000, 680, 850, 25, 65,
     ARRAY['self_employed'], 'For established self-employed applicants.', NOW()),
    ('trusted-credit.example', 'Young Professional Loan', 12.40, 30000, 690, 900, 21, 35,
     ARRAY['salaried'], 'Designed for early-career professionals.', NOW()),
    ('trusted-credit.example', 'Premium Personal Credit', 9.95, 100000, 780, 900, 28, 60,
     ARRAY['salaried', 'self_employed'], 'Lowest demo rate for high-income applicants.', NOW())
ON CONFLICT (source_site, product_name) DO UPDATE SET
    interest_rate = EXCLUDED.interest_rate,
    min_income = EXCLUDED.min_income,
    min_score = EXCLUDED.min_score,
    max_score = EXCLUDED.max_score,
    min_age = EXCLUDED.min_age,
    max_age = EXCLUDED.max_age,
    allowed_employment = EXCLUDED.allowed_employment,
    qualitative_notes = EXCLUDED.qualitative_notes,
    is_active = TRUE,
    last_seen_at = EXCLUDED.last_seen_at;
