# Examples

## Table of Contents

- [Basic Produce & Consume](#basic-produce--consume)
- [Typed Events with `.with()`](#typed-events-with-with)
- [Handling All Events with `*`](#handling-all-events-with-)
- [Serial Processing Mode](#serial-processing-mode)
- [Batch Producing with `produceMany`](#batch-producing-with-producemany)
- [Request/Reply Pattern](#requestreply-pattern)
- [Multiple Streams in One Group](#multiple-streams-in-one-group)
- [Redis Cluster](#redis-cluster)
- [Graceful Shutdown](#graceful-shutdown)

---

## Basic Produce & Consume

The simplest end-to-end example: one producer, one consumer.

```ts
import { RedisStreams } from 'ioredis-streams';

const streams = new RedisStreams('my-service', {
  redis: { host: 'localhost', port: 6379 },
});

const stream = streams.group('notifications').stream('alerts');

// Register handlers before consuming
stream.handle('alert.sent', async (id, event) => {
  console.log(`[${id}] Alert sent to`, event.data.recipient);
});

// Start consuming
await stream.consume();

// Produce an event (can happen in any order relative to consume)
await stream.produce({
  name: 'alert.sent',
  v: '1.0.0',
  time: Date.now(),
  data: { recipient: 'ops-team', message: 'Disk usage at 90%' },
});
```

---

## Typed Events with `.with()`

Use `event()` factories and `.with()` to get TypeScript inference on both producing and handling.

```ts
import { RedisStreams, event } from 'ioredis-streams';

// Define event schemas once
const OrderEvents = {
  ...event('order.placed').of<{ orderId: string; total: number; currency: string }>(),
  ...event('order.shipped').of<{ orderId: string; trackingCode: string }>(),
  ...event('order.cancelled').of<{ orderId: string; reason: string }>(),
};

const streams = new RedisStreams('order-service');

const stream = streams
  .group('order-processors')
  .stream('orders')
  .with(OrderEvents);

// Typed produce — data shape is enforced at compile time
await stream['order.placed']({ orderId: 'ORD-1', total: 49.99, currency: 'USD' });
await stream['order.shipped']({ orderId: 'ORD-1', trackingCode: 'TRK-XYZ' });

// Typed handle — event.data is fully inferred
stream
  .handle('order.placed', async (id, event) => {
    // event.data: { orderId: string; total: number; currency: string }
    await db.orders.create(event.data);
  })
  .handle('order.shipped', async (id, event) => {
    // event.data: { orderId: string; trackingCode: string }
    await notifications.send(`Order ${event.data.orderId} shipped`);
  });

await stream.consume();
```

---

## Handling All Events with `*`

Use `'*'` as a catch-all handler for events that have no specific handler registered. Runs after named handlers are checked.

```ts
stream
  .handle('order.placed', async (id, event) => {
    await db.orders.create(event.data);
  })
  .handle('*', async (id, event) => {
    // Catches order.shipped, order.cancelled, and anything else
    console.log('Unhandled event type:', event.name, event.data);
  });

await stream.consume();
```

> The `'*'` handler only fires if no named handler matched. If you register both `'order.placed'` and `'*'`, an `order.placed` event will only call the named handler.

---

## Serial Processing Mode

By default, messages in each batch are processed in **parallel**. Switch to `serial` mode to guarantee ordering — each message waits for the previous one to finish before processing begins.

```ts
const stream = streams
  .group('inventory')
  .stream('stock-updates', {
    mode: 'serial',   // one at a time, in order
    batchSize: 1,     // read one message per poll for strict FIFO
  });

stream.handle('stock.adjusted', async (id, event) => {
  // Guaranteed not to run concurrently with another stock.adjusted handler
  await db.inventory.adjust(event.data);
});

await stream.consume();
```

> **When to use serial mode:** Database writes with ordering guarantees, state machines, or any workflow where two concurrent messages would produce incorrect results.

---

## Batch Producing with `produceMany`

Use `produceMany` to buffer multiple events into a single Redis pipeline and flush them atomically.

```ts
import { RedisStreams, event } from 'ioredis-streams';

const MetricEvents = {
  ...event('metric.recorded').of<{ name: string; value: number; tags: Record<string, string> }>(),
};

const streams = new RedisStreams('metrics-service');
const stream = streams.group('metrics').stream('metrics').with(MetricEvents);

// Queue events — nothing is written to Redis yet
const batch = stream
  .produceMany(MetricEvents['metric.recorded']({ name: 'cpu', value: 72, tags: { host: 'web-1' } }))
  .produceMany(MetricEvents['metric.recorded']({ name: 'mem', value: 55, tags: { host: 'web-1' } }))
  .produceMany(MetricEvents['metric.recorded']({ name: 'disk', value: 90, tags: { host: 'web-1' } }));

// Write all three in one round-trip
await batch.flush();
```

> **Why use `produceMany`?** It reduces round-trips to Redis when you need to publish several events at once, e.g. at the end of a request lifecycle or a batch job.

---

## Request/Reply Pattern

Use `eventWithReply` when you need a synchronous-feeling response from a consumer. The producer publishes an event and awaits a reply over a Redis Pub/Sub channel. The consumer's handler return value becomes the reply.

### Producer service

```ts
import { RedisStreams, eventWithReply } from 'ioredis-streams';

const PricingEvents = {
  ...eventWithReply('pricing.calculate', 'order-service').of<
    { items: Array<{ sku: string; qty: number }> },
    { total: number; breakdown: Record<string, number> }
  >(),
};

const streams = new RedisStreams('order-service');
const stream = streams
  .group('pricing')
  .stream('pricing-requests')
  .with(PricingEvents);

// Produce the event and wait up to 5 seconds for the reply
const reply = await stream['pricing.calculate']({
  items: [{ sku: 'SKU-A', qty: 2 }, { sku: 'SKU-B', qty: 1 }],
}).wait(5000);

console.log('Total:', reply.total);
// → Total: 149.97
```

### Consumer service

```ts
import { RedisStreams } from 'ioredis-streams';

const streams = new RedisStreams('pricing-service');
const stream = streams.group('pricing').stream('pricing-requests');

stream.handle('pricing.calculate', async (id, event) => {
  const { items } = event.data;
  const breakdown: Record<string, number> = {};
  let total = 0;

  for (const item of items) {
    const price = await pricingDb.getPrice(item.sku) * item.qty;
    breakdown[item.sku] = price;
    total += price;
  }

  // Whatever you return here is sent back to the producer's .wait() call
  return { total, breakdown };
});

await stream.consume();
```

> **Timeouts:** If the consumer does not reply within the timeout passed to `.wait(ms)`, a `Timeout` error is thrown. Always wrap `.wait()` in try/catch for production use.

```ts
try {
  const reply = await stream['pricing.calculate']({ items }).wait(5000);
} catch (err) {
  if (err.message === 'Timeout') {
    // handle timeout
  }
  throw err;
}
```

---

## Multiple Streams in One Group

A single consumer group can consume from multiple streams. Each stream gets its own handlers.

```ts
import { RedisStreams, event } from 'ioredis-streams';

const streams = new RedisStreams('warehouse-service');
const group = streams.group('warehouse-processors');

// Stream 1: orders
const ordersStream = group.stream('orders').with({
  ...event('order.placed').of<{ orderId: string }>(),
});

ordersStream
  .handle('order.placed', async (id, event) => {
    await warehouse.reserveStock(event.data.orderId);
  });

await ordersStream.consume();

// Stream 2: returns
const returnsStream = group.stream('returns').with({
  ...event('return.initiated').of<{ orderId: string; reason: string }>(),
});

returnsStream
  .handle('return.initiated', async (id, event) => {
    await warehouse.processReturn(event.data.orderId);
  });

await returnsStream.consume();
```

---

## Redis Cluster

```ts
import { RedisStreams, event } from 'ioredis-streams';

const streams = new RedisStreams('my-service', {
  redis: {
    cluster: 'redis-node-1:6379,redis-node-2:6379,redis-node-3:6379',
  },
  clusterOptions: {
    redisOptions: {
      password: process.env.REDIS_PASSWORD,
      tls: {},
    },
    scaleReads: 'slave',
  },
});

const stream = streams
  .group('my-group')
  .stream('my-stream')
  .with({
    ...event('job.queued').of<{ jobId: string }>(),
  });

await stream['job.queued']({ jobId: 'job-123' });
```

---

## Graceful Shutdown

Stop the consumer cleanly on process exit to avoid mid-flight message loss.

```ts
import { RedisStreams, event } from 'ioredis-streams';

const streams = new RedisStreams('my-service');
const stream = streams.group('workers').stream('tasks');

stream.handle('task.run', async (id, event) => {
  await runTask(event.data);
});

const { stop } = await stream.consume();

// Graceful shutdown on SIGTERM / SIGINT
const shutdown = async () => {
  console.log('Shutting down...');
  stop(); // stops reading; in-flight handlers finish naturally
  process.exit(0);
};

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
```
