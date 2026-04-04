import 'mocha';
import { expect } from 'chai';
import { ConsumerBuffer, ConsumerBufferConfigs } from '../src/consumer-buffer';

function makeConfigs(overrides: Partial<ConsumerBufferConfigs> = {}): ConsumerBufferConfigs {
  return {
    ack: async () => {},
    process: async () => {},
    error: async () => {},
    mode: 'parallel',
    size: 10,
    ...overrides,
  };
}

describe('ConsumerBuffer', () => {

  describe('size getter', () => {
    it('returns the configured size', () => {
      const buf = new ConsumerBuffer(makeConfigs({ size: 7 }));
      expect(buf.size).to.equal(7);
    });
  });

  describe('parallel mode', () => {
    it('processes all messages and ACKs successful ones', async () => {
      const processed: string[] = [];
      const acked: string[] = [];
      const buf = new ConsumerBuffer(makeConfigs({
        mode: 'parallel',
        process: async (id) => { processed.push(id); },
        ack: async (...ids) => { acked.push(...ids); },
      }));
      await buf.add(['1', ['e', '{}']], ['2', ['e', '{}']]);
      expect(processed).to.have.members(['1', '2']);
      expect(acked).to.have.members(['1', '2']);
    });

    it('does not ACK messages whose processing throws', async () => {
      const acked: string[] = [];
      const buf = new ConsumerBuffer(makeConfigs({
        mode: 'parallel',
        process: async (id) => { if (id === '2') throw new Error('fail'); },
        ack: async (...ids) => { acked.push(...ids); },
      }));
      await buf.add(['1', ['e', '{}']], ['2', ['e', '{}']]);
      expect(acked).to.deep.equal(['1']);
    });

    it('calls the error handler when processing throws', async () => {
      const errorIds: string[] = [];
      const buf = new ConsumerBuffer(makeConfigs({
        mode: 'parallel',
        process: async () => { throw new Error('oops'); },
        error: async (id) => { errorIds.push(id); },
      }));
      await buf.add(['1', ['e', '{}']]);
      expect(errorIds).to.deep.equal(['1']);
    });

    it('swallows errors thrown by the error handler itself', async () => {
      const buf = new ConsumerBuffer(makeConfigs({
        mode: 'parallel',
        process: async () => { throw new Error('process error'); },
        error: async () => { throw new Error('error handler error'); },
      }));
      await expect(buf.add(['1', ['e', '{}']])).to.eventually.not.be.rejected;
    });

    it('does nothing when the buffer is empty', async () => {
      const acked: string[] = [];
      const buf = new ConsumerBuffer(makeConfigs({ ack: async (...ids) => { acked.push(...ids); } }));
      await buf.add();
      expect(acked).to.be.empty;
    });
  });

  describe('serial mode', () => {
    it('processes messages in insertion order', async () => {
      const order: string[] = [];
      const buf = new ConsumerBuffer(makeConfigs({
        mode: 'serial',
        process: async (id) => { order.push(id); },
      }));
      await buf.add(['1', ['e', '{}']], ['2', ['e', '{}']], ['3', ['e', '{}']]);
      expect(order).to.deep.equal(['1', '2', '3']);
    });

    it('ACKs each message immediately after processing succeeds', async () => {
      const log: string[] = [];
      const buf = new ConsumerBuffer(makeConfigs({
        mode: 'serial',
        process: async (id) => { log.push(`proc:${id}`); },
        ack: async (id) => { log.push(`ack:${id}`); },
      }));
      await buf.add(['1', ['e', '{}']], ['2', ['e', '{}']]);
      expect(log).to.deep.equal(['proc:1', 'ack:1', 'proc:2', 'ack:2']);
    });

    it('does not ACK when processing throws', async () => {
      const acked: string[] = [];
      const buf = new ConsumerBuffer(makeConfigs({
        mode: 'serial',
        process: async () => { throw new Error('fail'); },
        ack: async (id) => { acked.push(id); },
      }));
      await buf.add(['1', ['e', '{}']]);
      expect(acked).to.be.empty;
    });

    it('continues processing remaining messages after one fails', async () => {
      const processed: string[] = [];
      const acked: string[] = [];
      const buf = new ConsumerBuffer(makeConfigs({
        mode: 'serial',
        process: async (id) => {
          processed.push(id);
          if (id === '1') throw new Error('fail');
        },
        ack: async (id) => { acked.push(id); },
      }));
      await buf.add(['1', ['e', '{}']], ['2', ['e', '{}']]);
      expect(processed).to.deep.equal(['1', '2']);
      expect(acked).to.deep.equal(['2']);
    });

    it('swallows errors thrown by the error handler itself', async () => {
      const buf = new ConsumerBuffer(makeConfigs({
        mode: 'serial',
        process: async () => { throw new Error('process error'); },
        error: async () => { throw new Error('error handler error'); },
      }));
      await expect(buf.add(['1', ['e', '{}']])).to.eventually.not.be.rejected;
    });

    it('processes messages added to the buffer while a do() loop is already running', async () => {
      const processed: string[] = [];
      let bufRef!: ConsumerBuffer;
      bufRef = new ConsumerBuffer(makeConfigs({
        mode: 'serial',
        process: async (id) => {
          processed.push(id);
          if (id === '1') {
            // add msg2 while msg1 is being awaited — isDoing is true so do() returns immediately
            // but msg2 is still pushed to the buffer and picked up by the running while loop
            bufRef.add(['2', ['e', '{}']]);
          }
        },
      }));
      await bufRef.add(['1', ['e', '{}']]);
      expect(processed).to.deep.equal(['1', '2']);
    });
  });
});
