// eslint-disable-next-line @typescript-eslint/no-explicit-any

import { Point } from '@influxdata/influxdb3-client';

type AsyncFn = (...args: any[]) => Promise<any>;

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Class = { new (...args: any[]): any };

type Options = Partial<{
  monitorInvocations?: boolean;
  acceptedErrors?: Class[];
  thisArg?: any;
}>;

const startTimer = () => {
  const startTime = Date.now();
  return {
    elapsedMs: (): number => {
      return Date.now() - startTime;
    },
  };
};

export function monitorAsyncFunction<T extends AsyncFn>(
  operationName: string,
  call: T,
  options: Options = { monitorInvocations: true, acceptedErrors: [], thisArg: undefined },
): (...args: Parameters<T>) => Promise<Awaited<ReturnType<T>>> {
  const { monitorInvocations, acceptedErrors } = options;
  const point = Point.measurement(operationName);
  // for (const [key, value] of Object.entries(defaultTags)) {
  //   point.setTag(key, value);
  // }
  return async (...args: Parameters<T>) => {
    if (monitorInvocations) {
      console.log(`${operationName}-invocation`);
      point.setIntegerField('invocation', 1);
    }
    const timer = startTimer();
    try {
      return options.thisArg ? await call.bind(options.thisArg)(...args) : await call(...args);
    } catch (e) {
      if (!acceptedErrors || !acceptedErrors.some((acceptedError) => e instanceof acceptedError)) {
        console.log(`${operationName}-errors`);
        point.setIntegerField('errors', 1);
      }
      throw e;
    } finally {
      console.log(`${operationName}-duration`, timer.elapsedMs());
      point.setIntegerField('duration', timer.elapsedMs());
      console.log(point.toLineProtocol()?.toString());
      await fetch('https://cf-workers.monitoring.immich.cloud/write', {
        method: 'POST',
        body: point.toLineProtocol()?.toString(),
        headers: {
          'User-Agent': 'InfluxDBClient/1.0',
          Authorization: `Token YvGUMEDeN5UFk3iJjvGsGMcZNQmNQzlVszYmJkJ2`,
          Accept: '*/*',
          'Content-Type': 'application/x-www-form-urlencoded',
        },
        redirect: 'follow',
      })
        .then((response) => {
          console.log('url', response.url);
          console.log('status', response.status);
          console.log('location header', response.headers.get('Location'));
          for (const [key, value] of response.headers.entries()) {
            console.log('fetch headers', `${key}: ${value}`);
          }
        })
        .catch((error) => {
          console.error('error', error);
        });
    }
  };
}
