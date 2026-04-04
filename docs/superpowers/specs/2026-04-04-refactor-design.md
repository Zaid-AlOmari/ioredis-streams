# ioredis-streams Refactor Design

## Goal

Split the single `src/index.ts` into focused modules with clean boundaries, introduce a narrow `IRedisClient` interface for testability, and preserve the exact existing public API surface.

## File Structure

```
src/
  types.ts           — all exported types and interfaces
  redis-client.ts    — IRedisClient interface + connection factories
  consumer-buffer.ts — ConsumerBuffer class
  stream-consumer.ts — StreamConsumer class
  redis-streams.ts   — RedisStreams class (main entry point)
  events.ts          — event(), eventWithReply(), augmentEvents() helpers
  index.ts           — re-exports only, no logic
```

## IRedisClient Interface

Narrow interface covering only the ~12 methods actually used internally. Real ioredis instances satisfy it structurally. Tests pass plain fake objects.

Methods: `xgroup`, `xreadgroup`, `xpending`, `xclaim`, `xadd`, `xack`, `publish`, `subscribe`, `unsubscribe`, `on`, `off`, `disconnect`, `pipeline`, `multi`, `exec`.

`pipeline()` and `multi()` return `IRedisClient` so pipeline chaining works without importing ioredis types.

## Dependencies Between Modules

```
index.ts
  └── redis-streams.ts
        ├── redis-client.ts   (IRedisClient, getNewRedisClient)
        ├── stream-consumer.ts
        │     ├── consumer-buffer.ts
        │     └── types.ts
        └── types.ts
  └── events.ts
  └── types.ts
```

## Public API — No Changes

All exports from the current `src/index.ts` are re-exported from the new `src/index.ts` unchanged:
- `RedisStreams`, `IEvent`, `IWaitEvent`, `DeadLetterEvent`
- `RedisStreamsConfig`, `RedisStreamsInputConfig`, `StreamConfigs`
- `StreamGroupConsumer`, `ConsumerGroup`, `ConsumeFunctions`, `HandleFunction`
- `ProduceFunc`, `NamedEventHandler`
- `event`, `eventWithReply`
- `getNewRedisClient`, `getExistingRedisClient`

## Bug Fixes Preserved

All 6 fixes from prior session are carried over as-is into the new structure:
1. Serial mode only ACKs on success
2. Per-instance producer client (not module-level singleton)
3. Per-stream pipeline (not shared across streams on same instance)
4. XPENDING idle filter done in userland (Redis 6 compatible)
5. NOGROUP recovery resets checkBacklog and lastReadId
6. RedisStreamsConfig type reflects array form for cluster
