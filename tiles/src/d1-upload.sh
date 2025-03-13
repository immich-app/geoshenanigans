#!/bin/bash

# Set the directory containing the .sql files
SQL_DIR="./"

# Set the list of databases
DATABASES=("tiles-weur" "tiles-eeur" "tiles-wnam" "tiles-enam" "tiles-apac" "tiles-oc" "tiles")
MAX_RETRIES=10

# Function to upload a single .sql file to a single database
upload_sql() {
  local sql_file=$1
  local database=$2
  local retries=0
  local success=false

  while [ $retries -lt $MAX_RETRIES ]; do
    echo "Uploading $sql_file to $database (Attempt $((retries + 1)))"
    if wrangler d1 execute --remote "$database" --file "$sql_file"; then
      success=true
      break
    else
      echo "Failed to upload $sql_file to $database. Retrying..."
      retries=$((retries + 1))
      sleep 2  # Wait for 2 seconds before retrying
    fi
  done

  if [ "$success" = false ]; then
    echo "Failed to upload $sql_file to $database after $MAX_RETRIES attempts."
  fi
}

# Main script
cd "$SQL_DIR" || exit

# Get the list of .sql files
SQL_FILES=(sql/*.sql)

# Upload files to each database in parallel, but only one file at a time for each server
for sql_file in "${SQL_FILES[@]}"; do
  for database in "${DATABASES[@]}"; do
    upload_sql "$sql_file" "$database" &
  done
  wait
done

echo "All uploads started."
