import loggerFactory from '@log4js-node/log4js-api';
import type { ClusterOptions } from 'ioredis';
import type { RateLimitConfig } from './rate-limiter';
import type { Duration } from './duration';

export interface IEvent<T> {
  time: number;
  name: string;
  v: string;
  data: T;
}

export interface IWaitEvent<T> extends IEvent<T> {
  wait: { source: string };
}

export type DeadLetterEvent = IEvent<{
  id: string;
  message: string[];
  stream: string;
  group: string;
  /**
   * Set to `'too-old'` when the message was older than the stream's `maxMessageAge`.
   * Absent for dead letters caused by too many delivery attempts (unchanged format).
   */
  reason?: DeadLetterReason;
}>;

export type DeadLetterReason = 'too-old';

export type RedisStreamsConfig = {
  redis: { host: string; port: number } | { host: string; port: number }[];
  clusterOptions?: ClusterOptions;
  logger: loggerFactory.Logger;
  deadLetters?: {
    stream: string;
    maxRetries: number;
    maxSize: number;
  };
};

export type RedisStreamsInputConfig = Omit<RedisStreamsConfig, 'redis'> & {
  redis:
    | { host: string; port: number }
    | { cluster: string };
  clusterOptions?: ClusterOptions;
};

export type StreamConfigs = {
  readBlockTime: number;
  claimIdleTime: number;
  batchSize: number;
  mode: 'parallel' | 'serial';
  maxLen: number;
  /**
   * Optional cap on how fast this consumer takes messages off the stream,
   * e.g. `{ maxMessages: 100, duration: '1m' }` for 100 messages per minute.
   * Applies per consumer instance, not across the whole group.
   */
  rateLimit?: RateLimitConfig;
  /**
   * Optional maximum age of a message when the consumer picks it up, e.g. `'5m'`.
   * Older ("late") messages are sent to dead letters with reason `'too-old'` and
   * acked instead of being processed. Age is measured from the stream entry id,
   * i.e. when Redis received the message. Requires dead letters to be configured.
   */
  maxMessageAge?: Duration;
};

export type ConsumerConfigs = StreamConfigs & {
  peerName: string;
  streamName: string;
  groupName: string;
  logger?: loggerFactory.Logger;
  deadLetters?: {
    handler: (...event: DeadLetterEvent[]) => Promise<void>;
    maxRetries: number;
  };
};

export type NamedEventHandler<E = IEvent<any>> = (id: string, event: E) => Promise<any>;

export type ConsumeFunctions = {
  consume: () => Promise<{
    stop: () => Promise<void>;
    continue: () => void;
  }>;
};

type NamedEvent<T, N extends string> = IEvent<T> & { name: N };
type NamedWaitEvent<T, N extends string, ReplyType = any> = IWaitEvent<T> & { name: N };
// eslint-disable-next-line @typescript-eslint/no-unused-vars
type _ReplyType = any; // kept for NamedWaitEvent ReplyType usage below

export type ProduceFunc = (...events: IEvent<any>[]) => {
  produceMany: ProduceFunc;
  flush: () => Promise<void>;
};

export type StreamGroupConsumer = ConsumeFunctions & {
  handle: HandleFunction<any>;
  produce: (...events: (IEvent<any> | IWaitEvent<any>)[]) => Promise<void> | {
    wait: (timeout: number) => Promise<any>;
  };
  produceMany: ProduceFunc;
  with: <O extends AllowedFactories<O>>(events: O) => WithTypedHandlers<O>;
};

export type ConsumerGroup = {
  stream: (streamName: string, config?: Partial<StreamConfigs>) => StreamGroupConsumer;
};

type _ReturnType<T> = T extends (...args: any[]) => infer R ? R : any;
type ReplyTypeOf<T> = T extends NamedWaitEvent<any, string, infer Reply> ? Reply : any;
type DataOfHandler<T> = T extends (...args: any[]) => IEvent<infer R> ? R : any;
type ArgsOf<T> = T extends (...args: infer Args) => any ? Args : never;

export type AllowedFactories<T> = {
  [name in keyof T]: (...args: any[]) => IEvent<DataOfHandler<T[name]>> | IWaitEvent<DataOfHandler<T[name]>>;
};

export type HandleFunction<T> = <N extends keyof T | '*'>(
  event: N,
  handler: NamedEventHandler<N extends keyof T ? _ReturnType<T[N]> : IEvent<any>>
) => { handle: HandleFunction<T> } & ConsumeFunctions;

type WithTypedHandlers<T> = PromisifiedFunctionsMap<T> & {
  handle: HandleFunction<T>;
} & Omit<StreamGroupConsumer, 'handle'>;

type PromisifiedFunctionsMap<T> = {
  [func in keyof T]: (...args: ArgsOf<T[func]>) => _ReturnType<T[func]> extends { wait: { source: string } }
    ? { wait: (timeout: number) => Promise<ReplyTypeOf<_ReturnType<T[func]>>> }
    : Promise<void>;
};

export type { NamedEvent, NamedWaitEvent, WithTypedHandlers, PromisifiedFunctionsMap };
