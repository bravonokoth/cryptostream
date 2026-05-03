CREATE SCHEMA IF NOT EXISTS crypto_transform;
GRANT ALL ON SCHEMA crypto_transform TO PUBLIC;
-- Assuming a dbt_user exists, if not create one or grant to current user.
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO PUBLIC;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA crypto_transform TO PUBLIC;
