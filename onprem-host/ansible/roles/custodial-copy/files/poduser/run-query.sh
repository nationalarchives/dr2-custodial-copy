#!/bin/bash

# Set database and query
DB="/var/local/custodial-copy/database/reconciler"
QUERY="select p.id from PreservicaCOs p left join OcflCos o on p.sha256checksum = o.sha256checksum where o.sha256checksum is null limit 10;"

# Path to the script to call
SCRIPT="./check-asset.sh"

# Run the query, outputting results line by line
sqlite3 "$DB" "$QUERY" | while IFS= read -r entry; do
  # Call the other script with each entry as argument
  bash "$SCRIPT" "$entry"
done
