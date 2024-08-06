// eslint-disable-next-line @typescript-eslint/no-explicit-any
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

  return async (...args: Parameters<T>) => {
    if (monitorInvocations) {
      console.log(`${operationName}-invocation`);
      // await metrics.count(`${operationName}-invocation`)
    }
    const timer = startTimer();
    try {
      return options.thisArg ? await call.bind(options.thisArg)(...args) : await call(...args);
    } catch (e) {
      if (!acceptedErrors || !acceptedErrors.some((acceptedError) => e instanceof acceptedError)) {
        console.log(`${operationName}-errors`);
        // await metrics.count(`${operationName}-errors`)
      }
      throw e;
    } finally {
      console.log(`${operationName}-duration`, timer.elapsedMs());
      // await metrics.durationMilliseconds(
      //   `${operationName}-duration`,
      //   timer.elapsedMs(),
      // )
    }
  };
}
