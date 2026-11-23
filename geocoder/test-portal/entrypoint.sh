#!/bin/sh
# The test portal's container: query-portal (geocoder/server/src/portal.rs)
# syncs the whole planet build into $DATA_DIR/builds by itself, each file
# checked against the build's manifest, and serves every selection of it.
# This makes the test account the page uses, then hands over to it.
set -e

DATA_DIR="${DATA_DIR:-/data}"

# The test account, whose key the page sends. Every visitor shares it (it is
# not rated by IP); the page keeps 8 requests in flight at most and tries a
# 429 again, so 200 a second leaves room for a few pages at once.
if [ ! -f "$DATA_DIR/geocoder.json" ]; then
    echo "Creating test auth config..."
    mkdir -p "$DATA_DIR"
    cat > "$DATA_DIR/geocoder.json" << 'AUTHEOF'
{
  "users": {
    "test": {
      "password_hash": "",
      "admin": false,
      "rate_per_second": 200,
      "rate_per_day": 5000000,
      "rate_by_ip": false
    }
  },
  "tokens": {
    "test": "test"
  }
}
AUTHEOF
fi

exec query-portal "$DATA_DIR" 0.0.0.0:3000
