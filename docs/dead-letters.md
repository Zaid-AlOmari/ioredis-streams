# Dead Letters

When a message fails to process and is retried more times than `maxRetries` allows, it is moved to a **dead letter stream** instead of being retried indefinitely. This prevents poison-pill messages from blocking your consumer forever.

## How It Works

1. A consumer reads a message and the handler throws an error
2. The message is not acknowledged — Redis keeps it in the **Pending Entry List (PEL)** for the group
3. On the next poll cycle, another consumer (or the same one) claims idle messages via `XCLAIM`
4. Each time a message is claimed and retried, Redis increments its delivery counter
5. When the delivery counter reaches `maxRetries`, the message is written to the dead letter stream and acknowledged (removed from the PEL)

## Configuration

Enable dead letters on the `RedisStreams` instance:

```ts
import { RedisStreams } from 'ioredis-streams';

const streams = new RedisStreams('my-service', {
  redis: { host: 'localhost', port: 6379 },
  deadLetters: {
    stream: 'dead-letters',   // name of the Redis stream to write dead letters to
    maxRetries: 3,            // delivery attempts before a message is considered dead
    maxSize: 10000,           // MAXLEN applied to the dead letter stream itself
  },
});
```

> `maxRetries` counts **total delivery attempts**, not just failures. A message delivered 3 times (regardless of whether it errored each time) will be moved to dead letters on the 3rd claim cycle.

## Consuming Dead Letters

The dead letter stream is a regular Redis stream. Consume it the same way you consume any other stream:

```ts
import { RedisStreams } from 'ioredis-streams';
import type { DeadLetterEvent } from 'ioredis-streams';

const streams = new RedisStreams('dead-letter-processor', {
  redis: { host: 'localhost', port: 6379 },
});

const dlqStream = streams
  .group('dlq-processors')
  .stream('dead-letters'); // must match the stream name configured above

dlqStream.handle('deadMessage', async (id, event: DeadLetterEvent) => {
  const { stream, group, message, id: originalId } = event.data;

  console.error('Dead letter received:', {
    originalId,
    fromStream: stream,
    fromGroup: group,
    rawMessage: message,
  });

  // Options:
  // 1. Log to an alerting system (PagerDuty, Datadog, etc.)
  // 2. Store in a database for manual review
  // 3. Attempt manual replay after fixing the underlying bug
  await alerting.notify(`Dead letter in ${stream}/${group}: ${originalId}`);
  await db.deadLetters.insert({ originalId, stream, group, message, receivedAt: new Date() });
});

await dlqStream.consume();
```

## Dead Letter Event Shape

Every message written to the dead letter stream has this structure:

```ts
type DeadLetterEvent = IEvent<{
  id: string;        // The original Redis entry ID (e.g. '1693000000000-0')
  message: string[]; // The raw Redis stream message fields [fieldName, jsonValue]
  stream: string;    // The source stream name
  group: string;     // The consumer group name
}>;

// Top-level fields (inherited from IEvent):
// name: 'deadMessage'
// v: '1.0.0'
// time: <unix ms when the dead letter was written>
```

## Replaying a Dead Letter

To replay a dead letter, parse the original event from `message` and re-produce it to the source stream:

```ts
dlqStream.handle('deadMessage', async (id, event: DeadLetterEvent) => {
  const { stream, message } = event.data;

  // message is [eventName, jsonPayload]
  const [eventName, jsonPayload] = message;
  const originalEvent = JSON.parse(jsonPayload);

  // Re-produce to the original stream
  const sourceStream = streams.group('my-group').stream(stream);
  await sourceStream.produce(originalEvent);

  console.log(`Replayed ${eventName} back to ${stream}`);
});
```

> **Warning:** Only replay dead letters after the underlying bug is fixed. Replaying before the fix will result in the message becoming a dead letter again.

## Tuning Retry Behaviour

The `claimIdleTime` stream config controls how long a message must be idle before it can be claimed. Combined with `maxRetries`, this determines the total time before a message is dead-lettered:

```
max time before dead letter ≈ maxRetries × claimIdleTime
```

Example with defaults (`maxRetries: 3`, `claimIdleTime: 15000ms`):
- Message fails on first delivery
- After 15s idle it is claimed and retried (attempt 2)
- After another 15s idle it is claimed and retried (attempt 3)
- On the 3rd claim it is moved to dead letters → ~30s total

Adjust `claimIdleTime` per stream to match the expected processing time of your handlers:

```ts
const stream = streams
  .group('slow-jobs')
  .stream('background-tasks', {
    claimIdleTime: 60000,  // allow 60s before claiming — handler may be slow
    maxRetries: 5,         // try 5 times before dead-lettering
  });
```
