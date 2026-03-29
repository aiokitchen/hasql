-- Create replication user
CREATE USER replicator WITH REPLICATION ENCRYPTED PASSWORD 'replicator_pass';

-- Create test database and user
CREATE USER testuser WITH ENCRYPTED PASSWORD 'testpass';
CREATE DATABASE testdb OWNER testuser;

-- Create test table in testdb
\c testdb
CREATE TABLE IF NOT EXISTS test_data (
    id SERIAL PRIMARY KEY,
    value TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
GRANT ALL ON test_data TO testuser;
GRANT USAGE, SELECT ON SEQUENCE test_data_id_seq TO testuser;
