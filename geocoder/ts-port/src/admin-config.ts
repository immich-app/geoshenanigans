// Admin-level → response-field mapping.  Mirrors the JSON config the
// Rust server loads from data/admin-config.json (or its hardcoded
// fallback).  For the admin-only port we hard-code a sensible default
// matching Nominatim's typical behaviour; can be loaded dynamically
// later if we want full parity with the Rust server's per-country
// overrides.

export interface AdminLevelMapping {
  // Field name on AddressDetails. null = ignored at this level.
  field: string | null;
}

// Default admin-level → field. These are global defaults; the Rust
// server overrides per-country via its config (e.g. NYC L5 → city).
// Index 0..15 corresponds to admin_level 0..15.
export const DEFAULT_ADMIN_LEVEL_MAP: (string | null)[] = [
  null,        // 0
  null,        // 1
  "country",   // 2
  null,        // 3
  "state",     // 4
  "region",    // 5
  "county",    // 6
  "municipality", // 7
  "city",      // 8
  "borough",   // 9
  "suburb",    // 10
  null,        // 11 — postal_code, handled separately
  null,        // 12
  null,        // 13
  null,        // 14
  null,        // 15 — class='place' marker, builder-internal
];

// Maps the place_type_override byte stored on a polygon (when the
// builder linked an admin polygon to a place node) into a field name.
// Mirrors Rust place_type_to_field in main.rs.
export function placeTypeToField(pt: number): string | null {
  switch (pt) {
    case 1: return "city";
    case 2: return "town";
    case 3: return "village";
    case 4: return "suburb";
    case 5: return "neighbourhood";
    case 6: return "quarter";
    default: return null;
  }
}
