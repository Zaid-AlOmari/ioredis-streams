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
