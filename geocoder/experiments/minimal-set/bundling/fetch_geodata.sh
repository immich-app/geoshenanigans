#!/usr/bin/env bash
# Fetch the files immich-app/base-images server/Dockerfile (main: lines 78-82) ADDs, into ./geodata,
# and record compressed (transfer) and unpacked sizes.
set -euo pipefail
cd "$(dirname "$0")/geodata"
for u in \
  https://download.geonames.org/export/dump/cities500.zip \
  https://download.geonames.org/export/dump/admin1CodesASCII.txt \
  https://download.geonames.org/export/dump/admin2Codes.txt \
  https://download.geonames.org/export/dump/countryInfo.txt \
  https://raw.githubusercontent.com/nvkelso/natural-earth-vector/v5.1.2/geojson/ne_10m_admin_0_countries.geojson; do
  curl -sSLO -R -w "%{url_effective} bytes=%{size_download} http=%{http_code}\n" "$u"
done
/ssd/shrink/venv/bin/python -c "import zipfile; zipfile.ZipFile(\"cities500.zip\").extractall(\".\")"
ls -la --time-style=long-iso
