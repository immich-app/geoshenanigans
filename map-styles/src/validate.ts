import { validateStyleMin as validateLatest } from '@maplibre/maplibre-gl-style-spec';
import { validateStyleMin as validateOldest } from 'maplibre-gl-style-spec-oldest';

type Issue = { message: string; severity?: 'error' | 'warning' };
export type ClientSpec = { client: string; validate: (style: object) => Issue[] };

// Every Immich client in use loads these styles, and MapLibre GL JS drops a whole style it finds a property it doesn't
// know in. So each one must validate against the style spec of the oldest maplibre-gl an Immich release still ships
// (5.6, in Immich 3.1.0), which the maplibre-gl-style-spec-oldest alias pins, and against the latest spec, which
// @maplibre/maplibre-gl-style-spec follows for the newest clients.
export const CLIENT_SPECS: ClientSpec[] = [
  { client: 'oldest client spec', validate: (style) => validateOldest(style as Parameters<typeof validateOldest>[0]) },
  { client: 'latest spec', validate: (style) => validateLatest(style as Parameters<typeof validateLatest>[0]) },
];

// Throws on every error any spec returns. Newer specs also return warnings, for styles that render but likely not as
// meant (a legacy filter inside an expression); those are logged.
export const validateStyle = (name: string, style: object, specs: ClientSpec[] = CLIENT_SPECS) => {
  const errors: string[] = [];
  for (const { client, validate } of specs) {
    for (const { message, severity } of validate(style)) {
      if (severity === 'warning') {
        console.warn(`${name}: ${message} (${client})`);
      } else {
        errors.push(`${message} (${client})`);
      }
    }
  }
  if (errors.length > 0) {
    throw new Error(`${name}: ${errors.join('; ')}`);
  }
};
