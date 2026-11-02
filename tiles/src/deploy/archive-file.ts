import { open } from 'node:fs/promises';
import { Archive } from './archive';

/** Opens a PMTiles archive on local disk; close() releases the file. */
export async function openArchiveFile(path: string): Promise<{ archive: Archive; close: () => Promise<void> }> {
  const file = await open(path);
  const read = async (position: number, length: number) => {
    const buffer = Buffer.alloc(length);
    let read = 0;
    while (read < length) {
      const { bytesRead } = await file.read(buffer, read, length - read, position + read);
      if (bytesRead === 0) {
        throw new Error(`Unexpected end of ${path} reading ${length} bytes at ${position}`);
      }
      read += bytesRead;
    }
    return buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + length) as ArrayBuffer;
  };
  try {
    return { archive: await Archive.open(read), close: () => file.close() };
  } catch (error) {
    await file.close();
    throw new Error(`${path}: ${error instanceof Error ? error.message : String(error)}`);
  }
}
