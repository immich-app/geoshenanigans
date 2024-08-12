// eslint-disable-next-line @typescript-eslint/no-explicit-any

import { Point } from '@influxdata/influxdb-client';
import { IDeferredRepository } from './interface';

type AsyncFn = (...args: any[]) => Promise<any>;

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Class = { new (...args: any[]): any };

type Options = Partial<{
  monitorInvocations?: boolean;
  acceptedErrors?: Class[];
}>;

const startTimer = () => {
  const startTime = Date.now();
  return {
    elapsedMs: (): number => {
      return Date.now() - startTime;
    },
  };
};

export class Metrics {
  private static _instance: Metrics;
  private readonly deferredRepository: IDeferredRepository;
  private readonly request: Request;
  private readonly env: WorkerEnv;
  private readonly defaultTags: { [key: string]: string };
  private readonly operationPrefix: string;

  private constructor(
    operationPrefix: string,
    request: Request<unknown, IncomingRequestCfProperties>,
    deferredRepository: IDeferredRepository,
    env: WorkerEnv,
  ) {
    this.request = request;
    this.deferredRepository = deferredRepository;
    this.env = env;
    this.defaultTags = {
      colo: request.cf?.colo ?? '',
      rayId: request.headers.get('cf-ray') ?? '',
      asOrg: request.cf?.asOrganization ?? '',
      scriptTag: env.CF_VERSION_METADATA.tag,
      scriptId: env.CF_VERSION_METADATA.id,
    };
    this.operationPrefix = operationPrefix;
  }

  public static initialiseMetrics(
    operationPrefix: string,
    request: Request<unknown, IncomingRequestCfProperties>,
    deferredRepository: IDeferredRepository,
    env: WorkerEnv,
  ) {
    this._instance = new Metrics(operationPrefix, request, deferredRepository, env);
    return this._instance;
  }

  static getMetrics() {
    return this._instance;
  }

  monitorAsyncFunction<T extends AsyncFn>(
    operation: { name: string; extraTags?: { [key: string]: string } },
    call: T,
    options: Options = {},
  ): (...args: Parameters<T>) => Promise<Awaited<ReturnType<T>>> {
    const { name: operationName, extraTags = {} } = operation;
    const { monitorInvocations = true, acceptedErrors = [] } = options;

    const point = new Point(`${this.operationPrefix}_${operationName}`);
    for (const [key, value] of Object.entries({ ...this.defaultTags, ...extraTags })) {
      point.tag(key, value);
    }
    return async (...args: Parameters<T>) => {
      if (monitorInvocations) {
        point.intField('invocation', 1);
      }
      const timer = startTimer();
      return call(...args)
        .catch((e) => {
          if (!acceptedErrors || !acceptedErrors.some((acceptedError) => e instanceof acceptedError)) {
            console.error(e, `${operationName}_errors`);
            point.intField('errors', 1);
          }
          throw e;
        })
        .finally(() => {
          point.intField('duration', timer.elapsedMs());
          const influxLineProtocol = point.toLineProtocol()?.toString();
          if (this.env.ENVIRONMENT === 'production') {
            this.deferredRepository.defer(
              fetch('https://cf-workers.monitoring.immich.cloud/write', {
                method: 'POST',
                body: influxLineProtocol,
                headers: {
                  Authorization: `Token ${this.env.VMETRICS_API_TOKEN}`,
                },
              }),
            );
          } else {
            console.log(influxLineProtocol);
          }
        });
    };
  }
}
