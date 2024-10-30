#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "postgres" <<-EOSQL
    CREATE ROLE admin WITH LOGIN PASSWORD 'password';
    CREATE DATABASE etl_db OWNER admin;
    GRANT ALL PRIVILEGES ON DATABASE etl_db TO admin;
EOSQL
