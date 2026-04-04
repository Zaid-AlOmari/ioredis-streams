import 'mocha';
import { expect } from 'chai';
import { parseClusterString, getDefaultRedisConfigs, getNewRedisClient, getExistingRedisClient } from '../src/redis-client';

describe('parseClusterString()', () => {
  it('parses a single host:port entry', () => {
    const result = parseClusterString('redis1:6379');
    expect(result).to.deep.equal([{ host: 'redis1', port: 6379 }]);
  });

  it('parses multiple comma-separated host:port entries', () => {
    const result = parseClusterString('h1:6379,h2:6380,h3:6381');
    expect(result).to.deep.equal([
      { host: 'h1', port: 6379 },
      { host: 'h2', port: 6380 },
      { host: 'h3', port: 6381 },
    ]);
  });
});

describe('getDefaultRedisConfigs()', () => {
  let originalHost: string | undefined;
  let originalPort: string | undefined;
  let originalCluster: string | undefined;

  beforeEach(() => {
    originalHost = process.env.REDIS_HOST;
    originalPort = process.env.REDIS_PORT;
    originalCluster = process.env.REDIS_CLUSTER;
    delete process.env.REDIS_HOST;
    delete process.env.REDIS_PORT;
    delete process.env.REDIS_CLUSTER;
  });

  afterEach(() => {
    if (originalHost !== undefined) process.env.REDIS_HOST = originalHost;
    else delete process.env.REDIS_HOST;
    if (originalPort !== undefined) process.env.REDIS_PORT = originalPort;
    else delete process.env.REDIS_PORT;
    if (originalCluster !== undefined) process.env.REDIS_CLUSTER = originalCluster;
    else delete process.env.REDIS_CLUSTER;
  });

  it('returns parsed cluster nodes when REDIS_CLUSTER is set', () => {
    process.env.REDIS_CLUSTER = 'node1:7001,node2:7002';
    const result = getDefaultRedisConfigs();
    expect(result).to.deep.equal([
      { host: 'node1', port: 7001 },
      { host: 'node2', port: 7002 },
    ]);
  });

  it('throws when REDIS_HOST is not set', () => {
    expect(() => getDefaultRedisConfigs()).to.throw('REDIS_HOST');
  });

  it('throws when REDIS_HOST is an empty string', () => {
    process.env.REDIS_HOST = '';
    expect(() => getDefaultRedisConfigs()).to.throw('REDIS_HOST');
  });

  it('throws when REDIS_PORT is 0 or missing', () => {
    process.env.REDIS_HOST = 'localhost';
    expect(() => getDefaultRedisConfigs()).to.throw('REDIS_PORT');
  });

  it('returns a single host/port config when both env vars are set', () => {
    process.env.REDIS_HOST = 'my-redis';
    process.env.REDIS_PORT = '6379';
    const result = getDefaultRedisConfigs() as { host: string; port: number };
    expect(result.host).to.equal('my-redis');
    expect(result.port).to.equal(6379);
  });
});

describe('getNewRedisClient()', () => {
  it('returns a client-like object for a single-node config', () => {
    const client = getNewRedisClient({ host: 'localhost', port: 6399 });
    expect(client).to.exist;
    (client as any).disconnect();
  });

  it('returns a cluster client for an array config', () => {
    const client = getNewRedisClient([{ host: 'localhost', port: 7001 }]);
    expect(client).to.exist;
    (client as any).disconnect();
  });

  it('uses env-var default config when called without arguments', () => {
    process.env.REDIS_HOST = 'localhost';
    process.env.REDIS_PORT = '6399';
    try {
      const client = getNewRedisClient();
      expect(client).to.exist;
      (client as any).disconnect();
    } finally {
      delete process.env.REDIS_HOST;
      delete process.env.REDIS_PORT;
    }
  });
});

describe('getExistingRedisClient()', () => {
  it('uses env-var default config when called without arguments (first call)', () => {
    process.env.REDIS_HOST = 'localhost';
    process.env.REDIS_PORT = '6399';
    try {
      const client = getExistingRedisClient();
      expect(client).to.exist;
    } finally {
      delete process.env.REDIS_HOST;
      delete process.env.REDIS_PORT;
    }
  });

  it('returns the same cached instance on repeated calls', () => {
    const c1 = getExistingRedisClient({ host: 'localhost', port: 6400 });
    const c2 = getExistingRedisClient({ host: 'different-host', port: 9999 });
    expect(c1).to.equal(c2);
    (c1 as any).disconnect();
  });
});
