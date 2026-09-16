// display_name builder. Simplified port of the Rust format_address.
// For the admin-only port we do a US/CA-friendly default ordering;
// per-country format_rules are deferred.

import { Address, AddressDetails } from "./types.js";

// Field order for display_name from most-specific to most-general.
// Mirrors the typical Nominatim "address-line" ordering.
const DISPLAY_ORDER: (keyof AddressDetails)[] = [
  "house_number",
  "road",
  "landmark",
  "neighbourhood",
  "city_block",
  "quarter",
  "suburb",
  "city_district",
  "village",
  "hamlet",
  "town",
  "city",
  "borough",
  "municipality",
  "county",
  "state_district",
  "region",
  "province",
  "state",
  "country",
];

export function formatDisplayName(address: AddressDetails): string {
  const parts: string[] = [];
  const seen = new Set<string>();
  for (const key of DISPLAY_ORDER) {
    const v = address[key];
    if (typeof v === "string" && v.length > 0 && !seen.has(v)) {
      parts.push(v);
      seen.add(v);
    }
  }
  return parts.join(", ");
}

export function buildResponse(address: AddressDetails): Address {
  return {
    display_name: formatDisplayName(address),
    address,
  };
}
