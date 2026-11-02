import { Compression, Entry, Header, Metadata, TileType } from '../pmtiles/types';
import { bytesToHeader, decompress, deserializeIndex } from '../pmtiles/utils';

const HEADER_SIZE_BYTES = 127;

/** Reads `length` bytes at `offset`, failing rather than returning fewer. */
export type RangeReader = (offset: number, length: number) => Promise<ArrayBuffer>;

/** A PMTiles v3 archive read the same way the worker reads the deployed copy. See archive-file.ts for files. */
export class Archive {
  private constructor(
    private read: RangeReader,
    readonly header: Header,
    readonly metadata: Metadata,
    private rootEntries: Entry[],
  ) {}

  static async open(read: RangeReader): Promise<Archive> {
    const headerBytes = await read(0, HEADER_SIZE_BYTES);
    if (new DataView(headerBytes).getUint16(0, true) !== 0x4d50) {
      throw new Error('Not a PMTiles archive');
    }
    const header = await bytesToHeader(headerBytes);
    // The chunk layout relies on tile data being stored in directory order.
    if (!header.clustered) {
      throw new Error('Archive must be clustered');
    }
    // The worker serves tiles as gzip-encoded MVT without inspecting them.
    if (header.tileType !== TileType.Mvt || header.tileCompression !== Compression.Gzip) {
      throw new Error(`Expected gzip MVT tiles, got type ${header.tileType} compression ${header.tileCompression}`);
    }

    const metadataBytes = await decompress(
      await read(header.jsonMetadataOffset, header.jsonMetadataLength),
      header.internalCompression,
    );
    const metadata = JSON.parse(new TextDecoder().decode(metadataBytes)) as Metadata;
    const archive = new Archive(read, header, metadata, []);
    archive.rootEntries = await archive.readDirectory(header.rootDirectoryOffset, header.rootDirectoryLength);
    return archive;
  }

  readTileData(offset: number, length: number): Promise<ArrayBuffer> {
    return this.read(this.header.tileDataOffset + offset, length);
  }

  /** Every tile entry in tile-ID order, as runs of consecutive entries from the same directory. */
  async *tileEntries(): AsyncGenerator<Entry[]> {
    yield* this.walk(this.rootEntries);
  }

  private async *walk(entries: Entry[], depth = 0): AsyncGenerator<Entry[]> {
    if (depth > 3) {
      throw new Error('Directory nesting too deep');
    }
    let run: Entry[] = [];
    for (const entry of entries) {
      if (entry.runLength > 0) {
        run.push(entry);
        continue;
      }
      if (run.length > 0) {
        yield run;
        run = [];
      }
      yield* this.walk(await this.readLeaf(entry), depth + 1);
    }
    if (run.length > 0) {
      yield run;
    }
  }

  private readLeaf(entry: Entry) {
    return this.readDirectory(this.header.leafDirectoryOffset + entry.offset, entry.length);
  }

  private async readDirectory(offset: number, length: number): Promise<Entry[]> {
    const bytes = await decompress(await this.read(offset, length), this.header.internalCompression);
    const entries = deserializeIndex(bytes);
    if (entries.length === 0) {
      throw new Error(`Empty directory at offset ${offset}`);
    }
    return entries;
  }
}
