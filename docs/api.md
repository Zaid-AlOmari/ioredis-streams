# API Reference

## Table of Contents

- [RedisStreams](#redisstreams)
- [ConsumerGroup](#consumergroup)
- [StreamGroupConsumer](#streamgroupconsumer)
- [event()](#event)
- [eventWithReply()](#eventwithreply)
- [getNewRedisClient()](#getnewredisclient)
- [getExistingRedisClient()](#getexistingredisclient)
- [Types](#types)

---

## RedisStreams

The main entry point. Manages Redis connections and provides access to consumer groups.

```ts
class RedisStreams {
  constructor(peerName: string, config?: Partial<RedisStreamsInputConfig>)
  group(groupName: string): ConsumerGroup
}
```

### Constructor

| Parameter | Type | Description |
|---|---|---|
| `peerName` | `string` | Unique name for this service instance. Used as the consumer name in Redis and for logging. |
| `config` | `Partial<RedisStreamsInputConfig>` | Optional. Redis connection, logger, dead letter config. Falls back to env vars if omitted. |

```ts
// Minimal — reads REDIS_HOST and REDIS_PORT from env
const streams = new RedisStreams('order-service');

// With explicit config
const streams = new RedisStreams('order-service', {
  redis: { host: 'localhost', port: 6379 },
  deadLetters: {
    stream: 'dead-letters',
    maxRetries: 3,
    maxSize: 10000,
  },
});
```

### `.group(groupName)`

Returns a `ConsumerGroup` scoped to the given consumer group name.

| Parameter | Type | Description |
|---|---|---|
| `groupName` | `string` | The Redis consumer group name. Created automatically if it doesn't exist. |

**Returns:** `ConsumerGroup`

```ts
const group = streams.group('payments-processors');
```

---

## ConsumerGroup

Scoped to a single consumer group. Use it to access individual streams within that group.

```ts
type ConsumerGroup = {
  stream(streamName: string, config?: Partial<StreamConfigs>): StreamGroupConsumer
}
```

### `.stream(streamName, config?)`

Returns a `StreamGroupConsumer` for the given stream. Calling this multiple times with the same `streamName` returns the same instance.

| Parameter | Type | Description |
|---|---|---|
| `streamName` | `string` | The Redis stream key. |
| `config` | `Partial<StreamConfigs>` | Optional per-stream tuning. See [Stream Tuning Options](./configuration.md#stream-tuning-options). |

**Returns:** `StreamGroupConsumer`

```ts
const stream = streams
  .group('payments-processors')
  .stream('payments', { batchSize: 10, mode: 'serial' });
```

---

## StreamGroupConsumer

The core object. Produces events to a stream and registers handlers to consume them.

```ts
type StreamGroupConsumer = {
  produce(...events: IEvent<any>[]): Promise<void>
  produce(event: IWaitEvent<any>): { wait(timeout: number): Promise<any> }
  produceMany(...events: IEvent<any>[]): { produceMany: ProduceFunc; flush(): Promise<void> }
  handle(event: string | '*', handler: NamedEventHandler): { handle, consume }
  consume(): Promise<{ stop(): void; continue(): void }>
  with<O>(events: O): WithTypedHandlers<O>
}
```

### `.produce(...events)`

Publishes one or more events to the stream immediately.

- For regular events: returns `Promise<void>`
- For a single `IWaitEvent`: returns `{ wait(timeout): Promise<ReplyType> }` — see [Request/Reply](./examples.md#requestreply-pattern)

```ts
// Produce one event
await stream.produce({ name: 'order.placed', v: '1.0.0', time: Date.now(), data: { orderId: '99' } });

// Produce multiple events in one call
await stream.produce(
  { name: 'order.placed', v: '1.0.0', time: Date.now(), data: { orderId: '99' } },
  { name: 'order.placed', v: '1.0.0', time: Date.now(), data: { orderId: '100' } },
);
```

### `.produceMany(...events)`

Batches events into a Redis pipeline. Call `.flush()` to execute.

```ts
stream
  .produceMany(event1)
  .produceMany(event2)
  .produceMany(event3);

// Nothing is written until flush()
await stream.produceMany(event4).flush();
```

### `.handle(eventName, handler)`

Registers a handler for a named event. Chain multiple `.handle()` calls.

| Parameter | Type | Description |
|---|---|---|
| `eventName` | `string \| '*'` | Event name to match, or `'*'` to catch all events not matched by a named handler. |
| `handler` | `(id: string, event: IEvent<T>) => Promise<any>` | Async function called with the message ID and parsed event. |

**Returns:** `{ handle, consume }` — chainable

```ts
stream
  .handle('order.placed', async (id, event) => {
    await db.orders.insert(event.data);
  })
  .handle('order.cancelled', async (id, event) => {
    await db.orders.cancel(event.data.orderId);
  })
  .handle('*', async (id, event) => {
    console.warn('Unhandled event:', event.name);
  });
```

The `id` parameter is the Redis stream entry ID (e.g. `1693000000000-0`). Messages are automatically acknowledged after your handler resolves. If the handler throws, the message is **not** acknowledged and will be re-delivered.

### `.consume()`

Starts the consumer loop. Must be called after all `.handle()` registrations.

**Returns:** `Promise<{ stop(): void; continue(): void }>`

| Method | Description |
|---|---|
| `stop()` | Gracefully stops reading new messages. In-flight messages finish processing. |
| `continue()` | Resumes a stopped consumer without re-initializing. |

```ts
const { stop, continue: resume } = await stream.consume();

// Later
stop();
```

### `.with(eventFactories)`

Augments the stream object with typed producer methods derived from your event factories. This is the recommended way to produce events — it gives you full TypeScript inference.

| Parameter | Type | Description |
|---|---|---|
| `eventFactories` | `AllowedFactories<O>` | An object whose values are event factory functions (created via `event()` or `eventWithReply()`). |

**Returns:** `WithTypedHandlers<O>` — the stream object extended with a typed method for each factory

```ts
const OrderEvents = {
  ...event('order.placed').of<{ orderId: string; total: number }>(),
  ...event('order.cancelled').of<{ orderId: string }>(),
};

const stream = streams
  .group('order-processors')
  .stream('orders')
  .with(OrderEvents);

// Now you get typed, auto-completing produce methods:
await stream['order.placed']({ orderId: '42', total: 99.99 });
await stream['order.cancelled']({ orderId: '42' });
```

---

## event()

Creates a typed event factory for a named event.

```ts
function event<N extends string>(name: N, version?: string): {
  of<T>(): { [name]: (data: T, time?: number) => IEvent<T> }
}
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `string` | — | The event name. Used as the Redis stream field key. |
| `version` | `string` | `'1.0.0'` | Semantic version of the event schema. |

```ts
import { event } from 'ioredis-streams';

const UserEvents = {
  ...event('user.created').of<{ userId: string; email: string }>(),
  ...event('user.deleted', '2.0.0').of<{ userId: string }>(),
};

// UserEvents.['user.created']({ userId: '1', email: 'a@b.com' })
// → { name: 'user.created', v: '1.0.0', time: <now>, data: { userId: '1', email: 'a@b.com' } }
```

Use the spread (`...`) syntax when combining multiple `event()` factories into one object for `.with()`.

---

## eventWithReply()

Creates a typed event factory for a request/reply event. The consumer that handles this event can return a value which the producer receives via `.wait()`.

```ts
function eventWithReply<N extends string>(name: N, source: string, version?: string): {
  of<T, ReplyType>(): { [name]: (data: T, time?: number) => IWaitEvent<T> }
}
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | `string` | — | The event name. |
| `source` | `string` | — | A unique identifier for the reply channel (e.g. your service name or a request ID). |
| `version` | `string` | `'1.0.0'` | Semantic version. |

```ts
import { eventWithReply } from 'ioredis-streams';

const PricingEvents = {
  ...eventWithReply('pricing.calculate', 'order-service').of<
    { items: string[] },
    { total: number }   // ReplyType
  >(),
};
```

See [Request/Reply Pattern](./examples.md#requestreply-pattern) for a full example.

---

## getNewRedisClient()

Creates and returns a new ioredis client (or cluster client). Each call creates a new connection.

```ts
function getNewRedisClient(
  config?: RedisConfig,
  clusterOptions?: ClusterOptions,
): IRedisClient
```

```ts
import { getNewRedisClient } from 'ioredis-streams';

const client = getNewRedisClient({ host: 'localhost', port: 6379 });
```

---

## getExistingRedisClient()

Returns a cached singleton Redis client. The first call creates it; subsequent calls return the same instance.

> **Note:** This is a legacy export kept for backwards compatibility. Prefer `getNewRedisClient` for explicit connection management.

```ts
function getExistingRedisClient(
  config?: RedisConfig,
  clusterOptions?: ClusterOptions,
): IRedisClient
```

---

## Types

### `IEvent<T>`

The base event shape. All events produced and consumed by this library conform to this interface.

```ts
interface IEvent<T> {
  name: string;    // Event name / type identifier
  v: string;       // Schema version (e.g. '1.0.0')
  time: number;    // Unix timestamp in ms
  data: T;         // Event payload
}
```

### `IWaitEvent<T>`

Extends `IEvent<T>` with a `wait` field that enables the request/reply pattern.

```ts
interface IWaitEvent<T> extends IEvent<T> {
  wait: { source: string };
}
```

### `DeadLetterEvent`

The shape of events written to the dead letter stream.

```ts
type DeadLetterEvent = IEvent<{
  id: string;        // Original Redis stream entry ID
  message: string[]; // Raw stream message fields
  stream: string;    // Source stream name
  group: string;     // Consumer group name
}>;
```

### `RedisStreamsInputConfig`

```ts
type RedisStreamsInputConfig = {
  redis: { host: string; port: number } | { cluster: string };
  clusterOptions?: ClusterOptions;
  logger?: loggerFactory.Logger;
  deadLetters?: {
    stream: string;
    maxRetries: number;
    maxSize: number;
  };
}
```

### `StreamConfigs`

Per-stream tuning options passed to `.stream(name, config)`.

```ts
type StreamConfigs = {
  mode: 'parallel' | 'serial';
  batchSize: number;
  readBlockTime: number;
  claimIdleTime: number;
  maxLen: number;
}
```

### `NamedEventHandler<E>`

The signature for event handler functions passed to `.handle()`.

```ts
type NamedEventHandler<E = IEvent<any>> = (id: string, event: E) => Promise<any>;
```

### `ProduceFunc`

The return type of `.produceMany()`. Supports chaining and deferred execution.

```ts
type ProduceFunc = (...events: IEvent<any>[]) => {
  produceMany: ProduceFunc;
  flush: () => Promise<void>;
}
```
