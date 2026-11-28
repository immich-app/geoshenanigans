export interface PlaceSearchOptions {
  /** Overrides the search directory beside each dataset. Regional indexes go in subdirectories. */
  indexDir?: string;
  /** Sort memory in MiB when building the index. Defaults to 16. */
  buildMemoryMb?: number;
  /** Build threads. Defaults to 1. */
  threads?: number;
  profile?: 'immich';
}

export interface SearchPlacesOptions {
  /** Language preferences in Accept-Language syntax. */
  lang?: string;
  lat?: number;
  lon?: number;
  zoom?: number;
  bbox?: [minLon: number, minLat: number, maxLon: number, maxLat: number];
  /** Comma-separated ISO alpha-2 country codes. */
  countrycodes?: string;
  /** Comma-separated address, poi, railway, natural or manmade layers. */
  layer?: string;
}

export declare class Geocoder {
  private constructor();
  static load(dir: string): Geocoder;
  reverseGeocode(lat: number, lng: number): Promise<string>;
  openPlaceSearch(options?: PlaceSearchOptions): Promise<'current' | 'built'>;
  searchPlaces(query: string, limit?: number, options?: SearchPlacesOptions): Promise<string>;
}

export interface TransportFile {
  name: string;
  sha256: string;
}

export declare class TransportDecoder {
  private constructor();
  write(chunk: Buffer): Promise<void>;
  finish(): Promise<string[]>;
}

export declare function createTransportDecoder(dir: string, files: TransportFile[]): TransportDecoder;
