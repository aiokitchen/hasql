#!/bin/bash
set -e

# Wait for master to be ready
until PGPASSWORD=replicator_pass pg_isready -h pg-master -p 5432 -U replicator -d postgres; do
    echo "Waiting for master to be ready..."
    sleep 1
done

# If data directory is empty, do a base backup from master
if [ -z "$(ls -A "$PGDATA" 2>/dev/null)" ]; then
    echo "Performing base backup from master..."
    PGPASSWORD=replicator_pass pg_basebackup \
        -h pg-master \
        -p 5432 \
        -U replicator \
        -D "$PGDATA" \
        -Fp -Xs -P -R

    # Ensure hot_standby is on
    echo "hot_standby = on" >> "$PGDATA/postgresql.conf"

    chown -R postgres:postgres "$PGDATA"
    chmod 700 "$PGDATA"
fi

# Start postgres via the stock entrypoint
exec docker-entrypoint.sh postgres
