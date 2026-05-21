// Schema for configurations.json — see scripts/configurations.template.json.
//
// The file describes the cross-product of geocoder build axes (region,
// mode, quality, POI tier, …), what files belong to each axis value
// (`components`), what combinations are valid (`constraints`), and a
// few canned `presets` for the UI to offer as quick-start options.

export type AxisId = 'region' | 'mode' | 'quality' | 'poi_tier';
export type Selections = Partial<Record<AxisId, string>>;

export interface AxisValue {
  label: string;
  description?: string;
  order?: number;
  precision_m?: number;
  [key: string]: unknown;
}

export interface Axis {
  label: string;
  description?: string;
  order: number;
  values: Record<string, AxisValue>;
}

export interface Constraint {
  id: string;
  when: Record<string, string | string[]>;
  requires?: Record<string, string | string[]>;
  excludes?: Record<string, string | string[]>;
  message?: string;
}

export interface Preset {
  id: string;
  label: string;
  description?: string;
  selections: Selections;
  default?: boolean;
}

export interface Component {
  files: string[];
  extends?: string;
  replaces?: string[];
}

export interface FileEntry {
  size_zst: number;
  size_raw: number;
  sha256: string;
}

export interface Configurations {
  schema_version: number;
  build: {
    date: string;
    version: number;
    patch_version?: number;
    wikidata_date?: string;
    built_at?: string;
  };
  axes: Record<AxisId, Axis>;
  constraints: Constraint[];
  presets: Preset[];
  components: Record<string, Component>;
  files: Record<string, FileEntry>;
}

export interface Resolved {
  files: { path: string; size_zst: number; size_raw: number; sha256: string }[];
  total_zst: number;
  total_raw: number;
  missing: string[]; // file paths the manifest doesn't have an entry for
}
