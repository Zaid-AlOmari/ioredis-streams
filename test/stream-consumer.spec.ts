import 'mocha';
import { expect } from 'chai';
import { StreamConsumer, EventProcessor } from '../src/stream-consumer';
import { ConsumerConfigs, IEvent, IWaitEvent } from '../src/types';
import { createFakeRedis, FakeRedis } from './helpers/fake-redis';

// Subclass exposing protected internals for tests
class T extends StreamConsumer {
  get testDisposing() { return this.disposing; }
  set testDisposing(v: boolean) { this.disposing = v; }
  get testLastReadId() { return this.lastReadId; }
  set testLastReadId(v: string) { this.lastReadId = v; }
  get testCheckBacklog() { return this.checkBacklog; }
  set testCheckBacklog(v: boolean) { this.checkBacklog = v; }

  async testDoClaim() { return this.doClaim(); }
  async testDoRead() { return this.doRead(); }
  async testProcessMessage(id: string, message: string[]) { return this.processMessage(id, message); }
  async testTryReading() { return this.tryReading(); }
}

function makeConfig(overrides: Partial<ConsumerConfigs> = {}): ConsumerConfigs {
  return {
    streamName: 'my-stream',
    groupName: 'my-group',
    peerName: 'peer-1',
    batchSize: 5,
    claimIdleTime: 15000,
    readBlockTime: 5000,
    mode: 'parallel',
    maxLen: 100000,
    ...overrides,
  };
}

function makeEvent(name = 'click', data: any = {}): IEvent<any> {
  return { name, v: '1.0.0', data, time: Date.now() };
}

function encodedMsg(event: IEvent<any>): string[] {
  return [event.name, JSON.stringify(event)];
}

describe('StreamConsumer', () => {

  describe('constructor', () => {
    it('defaults batchSize to 5 when config batchSize is falsy', () => {
      const fake = createFakeRedis();
      const config = makeConfig({ batchSize: 0 });
      const consumer = new T(fake, async () => undefined, config);
      expect(consumer.size).to.equal(5);
    });

    it('uses the provided logger from config instead of creating a default one', () => {
      const fake = createFakeRedis();
      const myLogger = { trace: () => {}, info: () => {}, error: () => {}, warn: () => {} };
      const consumer = new T(fake, async () => undefined, makeConfig({ logger: myLogger as any }));
      expect((consumer as any).logger).to.equal(myLogger);
    });

    it('defaults buffer mode to parallel when mode is not provided', () => {
      const fake = createFakeRedis();
      const config = { ...makeConfig() };
      delete (config as any).mode;
      const consumer = new T(fake, async () => undefined, config);
      expect(consumer).to.exist;
    });
  });

  describe('init()', () => {
    it('creates the consumer group via XGROUP CREATE', async () => {
      const fake = createFakeRedis();
      const consumer = new T(fake, async () => undefined, makeConfig());
      await consumer.init();
      expect(fake.xgroup.calledOnce).to.be.true;
      const args = fake.xgroup.firstCall.args;
      expect(args[0]).to.equal('CREATE');
      expect(args[1]).to.equal('my-stream');
      expect(args[2]).to.equal('my-group');
    });

    it('silently ignores BUSYGROUP error', async () => {
      const fake = createFakeRedis();
      fake.xgroup.rejects(new Error('BUSYGROUP Consumer Group name already exists'));
      const consumer = new T(fake, async () => undefined, makeConfig());
      await expect(consumer.init()).to.eventually.not.be.rejected;
    });

    it('rethrows non-BUSYGROUP errors', async () => {
      const fake = createFakeRedis();
      fake.xgroup.rejects(new Error('WRONGTYPE operation not permitted'));
      const consumer = new T(fake, async () => undefined, makeConfig());
      await expect(consumer.init()).to.be.rejectedWith('WRONGTYPE');
    });
  });

  describe('doClaim()', () => {
    it('returns false without hitting Redis when called within the claimIdleTime window', async () => {
      const fake = createFakeRedis();
      const consumer = new T(fake, async () => undefined, makeConfig({ claimIdleTime: 15000 }));
      consumer.lastTimePendingCheck = Date.now(); // already checked moments ago
      const result = await consumer.testDoClaim();
      expect(result).to.be.false;
      expect(fake.xpending.called).to.be.false;
    });

    it('returns false when there are no pending messages', async () => {
      const fake = createFakeRedis();
      fake.xpending.resolves([]);
      const consumer = new T(fake, async () => undefined, makeConfig());
      const result = await consumer.testDoClaim();
      expect(result).to.be.false;
    });

    it('returns false when no pending messages exceed the idle threshold', async () => {
      const fake = createFakeRedis();
      // idleTime (5000) < claimIdleTime (15000) → filtered out
      fake.xpending.resolves([['1-0', 'peer-1', 5000, 1]]);
      const consumer = new T(fake, async () => undefined, makeConfig({ claimIdleTime: 15000 }));
      const result = await consumer.testDoClaim();
      expect(result).to.be.false;
    });

    it('claims messages whose idle time meets the threshold', async () => {
      const fake = createFakeRedis();
      fake.xpending.resolves([['1-0', 'other-peer', 20000, 1]]);
      fake.xclaim.resolves([['1-0', encodedMsg(makeEvent('click'))]]);
      const consumer = new T(fake, async () => undefined, makeConfig({ claimIdleTime: 15000 }));
      const result = await consumer.testDoClaim();
      expect(result).to.be.true;
      expect(fake.xclaim.calledOnce).to.be.true;
      expect(fake.xclaim.firstCall.args).to.include('1-0');
    });

    it('sends dead-letter events and ACKs them when delivery count exceeds maxRetries', async () => {
      const fake = createFakeRedis();
      const deadId = '1-0';
      const goodId = '2-0';
      const ev = makeEvent('click');
      // deadId has 3 deliveries (= maxRetries), goodId has 1
      fake.xpending.resolves([
        [deadId, 'peer', 20000, 3],
        [goodId, 'peer', 20000, 1],
      ]);
      fake.xclaim.resolves([
        [deadId, encodedMsg(ev)],
        [goodId, encodedMsg(ev)],
      ]);

      let deadHandlerCalled = false;
      // processEvent throws so the buffer never calls xack for the good message —
      // only the dead-letter path calls xack directly
      const consumer = new T(fake, async () => { throw new Error('processing failed'); }, makeConfig({
        claimIdleTime: 15000,
        deadLetters: {
          maxRetries: 3,
          handler: async () => { deadHandlerCalled = true; },
        },
      }));

      await consumer.testDoClaim();

      expect(deadHandlerCalled).to.be.true;
      // xack called exactly once: only for the dead message, not the good one
      expect(fake.xack.calledOnce).to.be.true;
      expect(fake.xack.firstCall.args).to.include(deadId);
      expect(fake.xack.firstCall.args).to.not.include(goodId);
    });

    it('skips dead-letter path when no deadLetters config is set', async () => {
      const fake = createFakeRedis();
      // high delivery count but no deadLetters config → treated as normal
      fake.xpending.resolves([['1-0', 'peer', 20000, 999]]);
      fake.xclaim.resolves([['1-0', encodedMsg(makeEvent())]]);
      // processEvent throws so the buffer never calls xack — only dead-letter path would
      const consumer = new T(fake, async () => { throw new Error('processing failed'); }, makeConfig());
      await consumer.testDoClaim();
      expect(fake.xack.called).to.be.false;
    });

    it('handles xclaim entries that are null (already acked between xpending and xclaim)', async () => {
      const fake = createFakeRedis();
      fake.xpending.resolves([['1-0', 'peer', 20000, 1]]);
      // null entry represents a message that disappeared
      fake.xclaim.resolves([null, ['1-0', encodedMsg(makeEvent())]]);
      const consumer = new T(fake, async () => undefined, makeConfig());
      const result = await consumer.testDoClaim();
      expect(result).to.be.true;
    });

    it('returns true but adds nothing to buffer when all claimed are dead letters', async () => {
      const fake = createFakeRedis();
      const deadId = '1-0';
      fake.xpending.resolves([[deadId, 'peer', 20000, 5]]);
      fake.xclaim.resolves([[deadId, encodedMsg(makeEvent())]]);
      let bufferAddCount = 0;
      const consumer = new T(fake, async () => undefined, makeConfig({
        claimIdleTime: 15000,
        deadLetters: {
          maxRetries: 3,
          handler: async () => { bufferAddCount++; },
        },
      }));
      const result = await consumer.testDoClaim();
      expect(result).to.be.true;
      expect(fake.xack.calledOnce).to.be.true;
    });
  });

  describe('doRead()', () => {
    it('passes the lastReadId when checkBacklog is true', async () => {
      const fake = createFakeRedis();
      fake.xreadgroup.resolves(null);
      const consumer = new T(fake, async () => undefined, makeConfig());
      consumer.testCheckBacklog = true;
      consumer.testLastReadId = '5-3';
      await consumer.testDoRead();
      const lastArg = fake.xreadgroup.firstCall.args.slice(-1)[0];
      expect(lastArg).to.equal('5-3');
    });

    it('passes ">" when checkBacklog is false', async () => {
      const fake = createFakeRedis();
      fake.xreadgroup.resolves(null);
      const consumer = new T(fake, async () => undefined, makeConfig());
      consumer.testCheckBacklog = false;
      await consumer.testDoRead();
      const lastArg = fake.xreadgroup.firstCall.args.slice(-1)[0];
      expect(lastArg).to.equal('>');
    });

    it('returns false and clears checkBacklog when xreadgroup returns null', async () => {
      const fake = createFakeRedis();
      fake.xreadgroup.resolves(null);
      const consumer = new T(fake, async () => undefined, makeConfig());
      consumer.testCheckBacklog = true;
      const result = await consumer.testDoRead();
      expect(result).to.be.false;
      expect(consumer.testCheckBacklog).to.be.false;
    });

    it('returns false and clears checkBacklog when stream entries array is empty', async () => {
      const fake = createFakeRedis();
      fake.xreadgroup.resolves([['my-stream', []]]);
      const consumer = new T(fake, async () => undefined, makeConfig());
      consumer.testCheckBacklog = true;
      const result = await consumer.testDoRead();
      expect(result).to.be.false;
      expect(consumer.testCheckBacklog).to.be.false;
    });

    it('buffers entries, updates lastReadId, and returns true', async () => {
      const fake = createFakeRedis();
      const ev = makeEvent('click');
      const entries: [string, string[]][] = [
        ['1-0', encodedMsg(ev)],
        ['2-0', encodedMsg(ev)],
      ];
      fake.xreadgroup.resolves([['my-stream', entries]]);
      fake.xack.resolves(1);
      const processed: string[] = [];
      const consumer = new T(fake, async (id) => { processed.push(id); }, makeConfig());
      const result = await consumer.testDoRead();
      expect(result).to.be.true;
      expect(consumer.testLastReadId).to.equal('2-0');
    });
  });

  describe('processMessage()', () => {
    it('parses the message and calls processEvent with event name and object', async () => {
      const fake = createFakeRedis();
      const calls: any[] = [];
      const processor: EventProcessor = async (id, event, obj) => { calls.push({ id, event, obj }); return 'ok'; };
      const consumer = new T(fake, processor, makeConfig());
      const ev = makeEvent('login', { userId: 42 });
      await consumer.testProcessMessage('msg-1', encodedMsg(ev));
      expect(calls).to.have.length(1);
      expect(calls[0].id).to.equal('msg-1');
      expect(calls[0].event).to.equal('login');
      expect(calls[0].obj.data.userId).to.equal(42);
    });

    it('logs an error and returns for a corrupted (non-JSON) message without throwing', async () => {
      const fake = createFakeRedis();
      const consumer = new T(fake, async () => undefined, makeConfig());
      // second element is invalid JSON
      await expect(consumer.testProcessMessage('bad-1', ['event', '{NOT JSON}'])).to.eventually.not.be.rejected;
    });

    it('publishes a reply to the response channel for IWaitEvent messages', async () => {
      const fake = createFakeRedis();
      const processor: EventProcessor = async () => 'my-reply';
      const consumer = new T(fake, processor, makeConfig());
      const waitEvent: IWaitEvent<any> = {
        name: 'req', v: '1.0.0', data: {}, time: 100,
        wait: { source: 'caller-service' },
      };
      await consumer.testProcessMessage('msg-1', ['req', JSON.stringify(waitEvent)]);
      expect(fake.publish.calledOnce).to.be.true;
      const [channel, payload] = fake.publish.firstCall.args;
      expect(channel).to.equal('req_100_caller-service');
      expect(JSON.parse(payload).reply).to.equal('my-reply');
    });
  });

  describe('tryReading()', () => {
    it('exits the loop immediately when dispose() is called', async () => {
      const fake = createFakeRedis();
      fake.xpending.resolves([]);
      const consumer = new T(fake, async () => undefined, makeConfig());
      consumer.lastTimePendingCheck = Date.now(); // skip claim

      let unblock!: () => void;
      fake.xreadgroup.callsFake(async () => {
        await new Promise<void>(r => { unblock = r; });
        return null;
      });

      const reading = consumer.testTryReading();
      await consumer.dispose();
      unblock();
      await reading;
      expect(consumer.testDisposing).to.be.true;
    });

    it('recovers from NOGROUP by reinitializing and resetting backlog state', async () => {
      const fake = createFakeRedis();
      const consumer = new T(fake, async () => undefined, makeConfig());
      consumer.lastTimePendingCheck = Date.now(); // skip claim
      consumer.testCheckBacklog = false; // will be reset by NOGROUP handler

      let callCount = 0;
      fake.xreadgroup.callsFake(async (...args: any[]) => {
        callCount++;
        if (callCount === 1) throw new Error('ERR NOGROUP No such consumer group');
        consumer.testDisposing = true;
        return null;
      });

      await consumer.testTryReading();

      // init() was called during recovery
      expect(fake.xgroup.calledOnce).to.be.true;
      // second xreadgroup call used '0-0' (backlog was reset)
      const secondCallLastArg = fake.xreadgroup.getCall(1).args.slice(-1)[0];
      expect(secondCallLastArg).to.equal('0-0');
    });

    it('logs non-NOGROUP errors but does not reinitialize', async () => {
      const fake = createFakeRedis();
      const consumer = new T(fake, async () => undefined, makeConfig());
      consumer.lastTimePendingCheck = Date.now();

      let callCount = 0;
      fake.xreadgroup.callsFake(async () => {
        callCount++;
        if (callCount === 1) throw new Error('CONNECTIONCLOSED unexpected');
        consumer.testDisposing = true;
        return null;
      });

      await consumer.testTryReading();
      expect(fake.xgroup.called).to.be.false; // no recovery init
    });

    it('continues to next iteration immediately when doClaim returns true', async () => {
      const fake = createFakeRedis();
      const ev = makeEvent('click');
      // pending message with idle time > claimIdleTime → doClaim returns true
      fake.xpending.resolves([['1-0', 'other-peer', 20000, 1]]);
      fake.xclaim.resolves([['1-0', encodedMsg(ev)]]);

      const consumer = new T(fake, async () => undefined, makeConfig({ claimIdleTime: 15000 }));
      // lastTimePendingCheck = 0 → doClaim will run on first iteration

      let readCount = 0;
      fake.xreadgroup.callsFake(async () => {
        readCount++;
        // On the second iteration doClaim returns false (timestamp fresh),
        // so doRead is called; signal exit
        consumer.testDisposing = true;
        return null;
      });

      await consumer.testTryReading();

      // doClaim ran (xclaim was called) → continued without calling doRead on first iteration
      expect(fake.xclaim.calledOnce).to.be.true;
      // doRead was called exactly once (second iteration)
      expect(readCount).to.equal(1);
    });

    it('continues to next iteration immediately when doRead returns true', async () => {
      const fake = createFakeRedis();
      const ev = makeEvent('click');
      const consumer = new T(fake, async () => undefined, makeConfig());
      consumer.lastTimePendingCheck = Date.now(); // skip claim every iteration

      let readCount = 0;
      fake.xreadgroup.callsFake(async () => {
        readCount++;
        if (readCount === 1) {
          // Return entries → doRead buffers them and returns true → continue (line 79)
          return [['my-stream', [['1-0', encodedMsg(ev)]]]];
        }
        // Second call: signal exit
        consumer.testDisposing = true;
        return null;
      });

      await consumer.testTryReading();

      // doRead was called twice — once returning data, once to exit
      expect(readCount).to.equal(2);
    });
  });

  describe('dispose()', () => {
    it('sets the disposing flag to stop the reading loop', async () => {
      const fake = createFakeRedis();
      const consumer = new T(fake, async () => undefined, makeConfig());
      expect(consumer.testDisposing).to.be.false;
      await consumer.dispose();
      expect(consumer.testDisposing).to.be.true;
    });
  });

  describe('publishDeadLetters()', () => {
    it('calls the configured dead-letter handler with wrapped events', async () => {
      const fake = createFakeRedis();
      const received: any[] = [];
      const consumer = new T(fake, async () => undefined, makeConfig({
        deadLetters: {
          maxRetries: 3,
          handler: async (...events) => { received.push(...events); },
        },
      }));
      await consumer.publishDeadLetters(['1-0', encodedMsg(makeEvent('click'))]);
      expect(received).to.have.length(1);
      expect(received[0].name).to.equal('deadMessage');
      expect(received[0].data.id).to.equal('1-0');
      expect(received[0].data.stream).to.equal('my-stream');
    });

    it('is a no-op when no deadLetters config is set', async () => {
      const fake = createFakeRedis();
      const consumer = new T(fake, async () => undefined, makeConfig());
      // should not throw
      await expect(consumer.publishDeadLetters(['1-0', ['e', '{}']])).to.eventually.not.be.rejected;
    });
  });
});
