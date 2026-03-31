resource "cloudflare_r2_bucket" "geocoder" {
  account_id = var.cloudflare_account_id
  name       = "geocoder"
  location   = "ENAM"
}

output "r2_bucket_name" {
  value = cloudflare_r2_bucket.geocoder.name
}

output "r2_s3_endpoint" {
  value = "https://${var.cloudflare_account_id}.r2.cloudflarestorage.com"
}

# R2 S3 API credentials must be created manually in the Cloudflare dashboard:
#   R2 > Manage R2 API Tokens > Create API Token
#   Permissions: Object Read & Write
#   Scope: Apply to specific bucket only > geocoder
#
# Then set these GitHub Actions secrets on immich-app/geoshenanigans:
#   GEOCODER_R2_BUCKET         = "geocoder"
#   GEOCODER_R2_ENDPOINT       = output.r2_s3_endpoint
#   GEOCODER_R2_ACCESS_KEY_ID  = <from dashboard>
#   GEOCODER_R2_SECRET_ACCESS_KEY = <from dashboard>
