#!/bin/bash
set -e
set -u

# Function to setup a user and their dedicated database
setup_db() {
  local database=$1
  local user=$2
  local password=$3
  echo "Setting up Database: $database for User: $user"

  # 1. Create User and Database (connecting to 'postgres' default db)
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE USER $user WITH PASSWORD '$password';
    CREATE DATABASE $database;
    GRANT ALL PRIVILEGES ON DATABASE $database TO $user;
EOSQL

  # 2. Grant Public Schema Permissions (Must connect to the NEW database)
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$database" <<-EOSQL
    GRANT ALL PRIVILEGES ON SCHEMA public TO $user;
EOSQL
}

# Define your databases and users
setup_db $METADATA_DATABASE_NAME $METADATA_DATABASE_USERNAME $METADATA_DATABASE_PASSWORD

setup_db $CELERY_BACKEND_NAME $CELERY_BACKEND_USERNAME $METADATA_DATABASE_PASSWORD

setup_db $ELT_DATABASE_NAME $ELT_DATABASE_USERNAME $ELT_DATABASE_PASSWORD

echo "All databases and users created successfully"