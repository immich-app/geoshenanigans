// The geocoder index directory every bench reads, from GEOCODER_DATA.
export function requireDataDir(): string {
  const dir = process.env.GEOCODER_DATA;
  if (!dir) throw new Error("Set GEOCODER_DATA to a geocoder index directory");
  return dir;
}
