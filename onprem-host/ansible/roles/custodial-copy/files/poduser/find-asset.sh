#!/usr/bin/env bash
set -euo pipefail

print_help() {
  cat <<EOF
Usage: $(basename "$0") <assetId>

Arguments:
  assetId      The ID of the asset to check.

Options:
  -h, --help   Show this help message and exit.

Example:
  $(basename "$0") daadb3b4-4237-4084-b092-d2603b81a360
EOF
}

# Show help if no arguments or help flag is passed
if [[ $# -eq 0 || "$1" == "-h" || "$1" == "--help" ]]; then
  print_help
  exit 1
fi

ASSET_ID="$1"

CREDS_JSON=$(/usr/local/bin/aws secretsmanager get-secret-value --secret-id "$CC_AWS_ENV"-preservica-api-read-metadata-read-content | jq '.SecretString | fromjson')
USERNAME=$(echo "$CREDS_JSON" | jq -r .userName)
PASSWORD=$(echo "$CREDS_JSON" | jq -r '.password | @uri')

AUTH_RESPONSE=$(curl -sS -X 'POST' \
  'https://tna.preservica.com/api/accesstoken/login' \
  -H 'accept: application/json' \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d "username=$USERNAME&password=$PASSWORD&cookie=false&includeUserDetails=false")

TOKEN=$(echo $AUTH_RESPONSE | jq -r '.token')

ENTITY_RESPONSE=$(curl -sS -X 'GET' \
  "https://tna.preservica.com/api/entity/entities/by-identifier?type=SourceID&value=$ASSET_ID" \
  -H 'accept: application/xml;charset=UTF-8' \
  -H "Preservica-Access-Token: $TOKEN")
echo $ENTITY_RESPONSE
ENTITY_REF=$(echo $ENTITY_RESPONSE | xmllint --xpath "//*[local-name()='Entity']/@ref" - | sed -E 's/ref="([^"]+)"/\1/')
#echo $ENTITY_REF

cd /mnt/dr2_cc/repo

rocfl ls $ENTITY_REF

