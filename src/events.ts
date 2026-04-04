import { AllowedFactories, IEvent, IWaitEvent, StreamGroupConsumer } from './types';

type NamedEvent<T, N extends string> = IEvent<T> & { name: N };
type NamedWaitEvent<T, N extends string, ReplyType = any> = IWaitEvent<T> & { name: N };

export const event = <N extends string>(name: N, v = '1.0.0') => {
  type Result<T, K extends string> = { [P in K]: (data: T, time?: number) => NamedEvent<T, K> };
  return {
    of: <T>() => ({
      [name]: (data: T, time = Date.now()) => ({ name, v, data, time }),
    } as Result<T, N>),
  };
};

export const eventWithReply = <N extends string>(name: N, source: string, v = '1.0.0') => {
  type Result<T, K extends string, R> = { [P in K]: (data: T, time?: number) => NamedWaitEvent<T, K, R> };
  return {
    of: <T, ReplyType = any>() => ({
      [name]: (data: T, time = Date.now()): IWaitEvent<T> => ({
        name, v, data, time, wait: { source },
      }),
    } as Result<T, N, ReplyType>),
  };
};

export const augmentEvents = <T extends AllowedFactories<R>, R>(
  events: T,
  stream: StreamGroupConsumer,
) => {
  return Object.keys(events).reduce((acc, key) => {
    const factory = events[key as keyof T];
    acc[key] = (...args: any[]) => stream.produce(factory(...args));
    return acc;
  }, { ...events, ...stream } as any);
};
