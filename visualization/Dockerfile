# Start with the official Metabase image
FROM metabase/metabase

# Set environment variables for Metabase
ENV MB_DB_TYPE=postgres
ENV MB_DB_DBNAME=etl_db
ENV MB_DB_PORT=5432
ENV MB_DB_USER=admin
ENV MB_DB_PASS=password
ENV MB_DB_HOST=db

# Expose Metabase port
EXPOSE 3000

