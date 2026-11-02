import { GetObjectCommand, ListObjectsV2Command, PutObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { requireEnv } from './config';

/** The environment's area of the tiles bucket: `<STORAGE_PREFIX><deployment key>/<name>`, as the worker reads it. */
export class DeploymentStorage {
  private constructor(
    private client: S3Client,
    private bucket: string,
    private prefix: string,
  ) {}

  static fromEnv() {
    const client = new S3Client({
      region: 'auto',
      endpoint: requireEnv('TIGRIS_ENDPOINT'),
      // Only for local S3 stand-ins; Tigris itself is addressed virtual-host style, like the worker does.
      forcePathStyle: process.env.S3_FORCE_PATH_STYLE === 'true',
      credentials: { accessKeyId: requireEnv('TIGRIS_KEY_ID'), secretAccessKey: requireEnv('TIGRIS_ACCESS_KEY') },
      maxAttempts: 10,
      retryMode: 'adaptive',
      requestChecksumCalculation: 'WHEN_REQUIRED',
      responseChecksumValidation: 'WHEN_REQUIRED',
    });
    return new DeploymentStorage(client, requireEnv('TIGRIS_BUCKET'), process.env.STORAGE_PREFIX ?? '');
  }

  key(deploymentKey: string, name: string) {
    return `${this.prefix}${deploymentKey}/${name}`;
  }

  async put(deploymentKey: string, name: string, body: Uint8Array, contentType?: string) {
    await this.client.send(
      new PutObjectCommand({
        Bucket: this.bucket,
        Key: this.key(deploymentKey, name),
        Body: body,
        ContentType: contentType,
      }),
    );
  }

  /** Every object of a deployment, by name, with its size. */
  async list(deploymentKey: string): Promise<Map<string, number>> {
    const prefix = this.key(deploymentKey, '');
    const objects = new Map<string, number>();
    let ContinuationToken: string | undefined;
    do {
      const page = await this.client.send(
        new ListObjectsV2Command({ Bucket: this.bucket, Prefix: prefix, ContinuationToken }),
      );
      for (const object of page.Contents ?? []) {
        objects.set(object.Key!.slice(prefix.length), object.Size ?? 0);
      }
      ContinuationToken = page.IsTruncated ? page.NextContinuationToken : undefined;
    } while (ContinuationToken);
    return objects;
  }

  async get(deploymentKey: string, name: string, range?: { offset: number; length: number }): Promise<Uint8Array> {
    const response = await this.client.send(
      new GetObjectCommand({
        Bucket: this.bucket,
        Key: this.key(deploymentKey, name),
        Range: range ? `bytes=${range.offset}-${range.offset + range.length - 1}` : undefined,
      }),
    );
    if (!response.Body) {
      throw new Error(`Empty response for ${this.key(deploymentKey, name)}`);
    }
    return response.Body.transformToByteArray();
  }
}
