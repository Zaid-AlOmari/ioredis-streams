# ioredis-streams

[![npm version](https://img.shields.io/npm/v/ioredis-streams)](https://www.npmjs.com/package/ioredis-streams)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A TypeScript library for working with **Redis Streams** using consumer groups, typed events, and a fluent API. Built on top of [ioredis](https://github.com/luin/ioredis).

## Features

- Fluent, chainable API for producing and consuming stream events
- Fully typed event factories with TypeScript generics
- Consumer groups with **parallel** or **serial** processing modes
- Automatic backlog recovery and idle message claiming
- Request/reply pattern with `eventWithReply` and `.wait()`
- Dead letter queue for messages that exceed retry limits
- Redis Cluster support

## Install

```bash
npm install ioredis-streams
```

## Quick Start

```ts
import { RedisStreams, event } from 'ioredis-streams';

// Define typed event factories
const UserEvents = {
  ...event('user.created').of<{ userId: string; email: string }>(),
  ...event('user.deleted').of<{ userId: string }>(),
};

// Create the streams client (reads REDIS_HOST / REDIS_PORT from env by default)
const streams = new RedisStreams('my-service');

// Get a typed stream handle
const userStream = streams
  .group('user-processors')
  .stream('users')
  .with(UserEvents);

// Produce an event
await userStream['user.created']({ userId: '42', email: 'zaid@example.com' });

// Consume events
userStream
  .handle('user.created', async (id, event) => {
    console.log('New user:', event.data.userId);
  })
  .handle('user.deleted', async (id, event) => {
    console.log('Deleted user:', event.data.userId);
  });

const { stop } = await userStream.consume();

// Later, to shut down gracefully:
stop();
```

## Documentation

| Topic | Description |
|---|---|
| [Configuration](docs/configuration.md) | Redis connection, env vars, cluster setup, stream tuning |
| [API Reference](docs/api.md) | All classes, methods, and TypeScript types |
| [Examples](docs/examples.md) | Real-world patterns: pub/sub, request/reply, batch producing |
| [Dead Letters](docs/dead-letters.md) | Handling messages that exceed retry limits |

## Author

**Zaid Al-Omari**

## License

MIT
