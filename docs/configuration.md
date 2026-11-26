# Configuration

## Redis Connection

`RedisStreams` resolves its Redis connection in this order:

1. **Explicit config** passed to the constructor
2. **Environment variables** (fallback when no config is passed)

### Environment Variables

| Variable | Description | Example |
|---|---|---|
| `REDIS_HOST` | Redis host | `localhost` |
| `REDIS_PORT` | Redis port | `6379` |
| `REDIS_CLUSTER` | Comma-separated list of cluster nodes | `host1:6379,host2:6380` |

If `REDIS_CLUSTER` is set it takes precedence over `REDIS_HOST`/`REDIS_PORT`.

### Passing Config Explicitly

```ts
import { RedisStreams } from 'ioredis-streams';

// Single node
const streams = new RedisStreams('my-service', {
  redis: { host: 'localhost', port: 6379 },
});

// Cluster via connection string
const streams = new RedisStreams('my-service', {
  redis: { cluster: 'node1:6379,node2:6380,node3:6381' },
});
```

### Redis Cluster with Custom Options

Pass `clusterOptions` to control TLS, NAT mapping, retry strategies, and other ioredis `ClusterOptions`:

```ts
import { RedisStreams } from 'ioredis-streams';

const streams = new RedisStreams('my-service', {
  redis: { cluster: 'node1:6379,node2:6380' },
  clusterOptions: {
    redisOptions: {
      tls: {},
      password: process.env.REDIS_PASSWORD,
    },
    enableReadyCheck: true,
  },
});
```

---

## Logger

The library uses `@log4js-node/log4js-api` for logging. By default it creates a logger named after the `peerName` you pass to the constructor. You can supply your own:

```ts
import loggerFactory from '@log4js-node/log4js-api';
import { RedisStreams } from 'ioredis-streams';

const logger = loggerFactory.getLogger('custom-name');

const streams = new RedisStreams('my-service', { logger });
```

---

## Stream Tuning Options

These can be set per-stream when calling `.stream(name, options)`:

| Option | Type | Default | Description |
|---|---|---|---|
| `mode` | `'parallel' \| 'serial'` | `'parallel'` | Whether messages in a batch are processed concurrently or one at a time |
| `batchSize` | `number` | `5` | How many messages to read per poll |
| `readBlockTime` | `number` (ms) | `5000` | How long to block waiting for new messages |
| `claimIdleTime` | `number` (ms) | `15000` | How long a message must be idle before another consumer can claim it |
| `maxLen` | `number` | `100000` | Maximum number of entries the stream will hold (trimmed with `MAXLEN ~`) |
| `rateLimit` | `{ maxMessages: number; duration: Duration }` | not set (no limit) | Caps how many messages this consumer takes per `duration` (see below) |
| `maxMessageAge` | `Duration` | not set (no age limit) | Messages older than this when picked up go to dead letters instead of being processed (see below) |

```ts
const stream = streams
  .group('my-group')
  .stream('orders', {
    mode: 'serial',       // process one at a time
    batchSize: 10,
    readBlockTime: 2000,
    claimIdleTime: 30000,
    maxLen: 50000,
  });
```

### Durations

`rateLimit.duration` and `maxMessageAge` take a number of milliseconds or a string with a unit: `'500ms'`, `'10s'`, `'1m'`, `'2h'`, `'1d'` (decimals like `'1.5s'` work too).

### Rate limiting consumption

Both options below are **optional and off by default**. If `rateLimit` is not set, the consumer reads as fast as it can; if `maxMessageAge` is not set, every message is eventually processed no matter how old it is.

Set `rateLimit` to cap how fast a consumer takes messages off a stream, for example to protect a downstream API or database:

```ts
const stream = streams
  .group('my-group')
  .stream('orders', {
    batchSize: 10,
    rateLimit: { maxMessages: 100, duration: '1m' }, // at most 100 messages per minute
  });
```

- It is a token bucket: the bucket holds `maxMessages` tokens and refills continuously at `maxMessages / duration`. Up to `maxMessages` can be taken in a burst, then messages are taken at that average rate. Its memory is a few numbers per consumer, whatever the limit.
- Because of bursts, a single `duration` window can see up to 2 × `maxMessages` (a full bucket plus what refills during the window). The long-run average never exceeds the limit.
- The consumer always honours `batchSize`: it waits until a full batch is allowed (`batchSize`, or `maxMessages` if that is smaller) and then fetches it in one Redis call. Throttled messages stay in Redis instead of waiting in memory.
- `countOn` sets when a message counts against the limit:
  - `'read'` (default): when it is taken from Redis. A failed message stays counted, and each retry counts again. Use this to protect a downstream system from every attempt.
  - `'ack'`: a spot is held when the message is taken from Redis, and given back if the handler fails (the message is not acked). Only successfully handled messages use up the limit.

  ```ts
  rateLimit: { maxMessages: 100, duration: '1m', countOn: 'ack' }
  ```
- The limit applies to each consumer instance. With 3 instances of a service in the same group, the group as a whole can consume up to 3 × the limit.
- Messages sent to dead letters (too many retries, or too old) do not count against the limit.

### Late messages (`maxMessageAge`)

Set `maxMessageAge` to stop processing messages that are too old to be useful by the time the consumer gets to them, e.g. because of a backlog or a rate limit:

```ts
const stream = streams
  .group('my-group')
  .stream('notifications', {
    maxMessageAge: '5m', // anything older than 5 minutes is a late message
  });
```

- Age is measured from the message's stream id, i.e. when Redis received it.
- The check runs when the consumer picks a message up, both on normal reads and when claiming idle messages from other consumers.
- A late message is written to the dead letter stream with `reason: 'too-old'` and acknowledged, so it is not processed by this group.
- Dead letters must be configured on the `RedisStreams` instance; setting `maxMessageAge` without them throws when the stream is created.

---

## Dead Letter Queue

Configure globally on the `RedisStreams` instance. Messages that fail more than `maxRetries` times are forwarded to the dead letter stream.

```ts
const streams = new RedisStreams('my-service', {
  redis: { host: 'localhost', port: 6379 },
  deadLetters: {
    stream: 'dead-letters',  // stream name to write failed messages to
    maxRetries: 3,           // number of delivery attempts before moving to dead letters
    maxSize: 10000,          // max entries in the dead letter stream
  },
});
```

See [Dead Letters](./dead-letters.md) for full details on consuming and handling dead letter events.

---

## Bringing Your Own Redis Client

If you already manage a Redis connection elsewhere, you can use it directly:

```ts
import { getNewRedisClient, getExistingRedisClient } from 'ioredis-streams';

// Always creates a new connection
const client = getNewRedisClient({ host: 'localhost', port: 6379 });

// Returns a cached singleton (legacy, kept for backwards compatibility)
const shared = getExistingRedisClient({ host: 'localhost', port: 6379 });
```
