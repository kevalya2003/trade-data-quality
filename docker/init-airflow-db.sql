-- Airflow keeps its own metadata separate from the data it orchestrates. Same server
-- for convenience in a demo; in production these would not share an instance, because
-- a heavy analytical query should not be able to slow down the scheduler.
CREATE DATABASE airflow OWNER trades;
