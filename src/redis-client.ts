import Redis from 'ioredis';

export interface IRedisClient {
  xgroup(...args: any[]): Promise<any>;
  xreadgroup(...args: any[]): Promise<any>;
  xpending(...args: any[]): Promise<any>;
  xclaim(...args: any[]): Promise<any>;
  xadd(...args: any[]): Promise<any>;
  xack(stream: string, group: string, ...ids: string[]): Promise<number>;
  publish(channel: string, message: string): Promise<number>;
  subscribe(channel: string): Promise<any>;
  unsubscribe(channel: string): Promise<any>;
  on(event: string, listener: (...args: any[]) => void): this;
  off(event: string, listener: (...args: any[]) => void): this;
  disconnect(): void;
  pipeline(): IRedisClient;
  multi(): IRedisClient;
  exec(): Promise<any>;
}

export type RedisConfig = { host: string; port: number } | { host: string; port: number }[];

export const parseClusterString = (clusterStr: string): { host: string; port: number }[] =>
  clusterStr
    .split(',')
    .map(x => x.split(':'))
    .map(([host, port]) => ({ host, port: parseInt(port) }));

// Read env vars inside the function so tests can manipulate process.env without module re-loading
export const getDefaultRedisConfigs = (): RedisConfig => {
  const redisCluster = process.env.REDIS_CLUSTER;
  const redisHost = process.env.REDIS_HOST;
  const redisPort = parseInt(process.env.REDIS_PORT || '0');

  if (redisCluster) {
    return parseClusterString(redisCluster);
  }
  if (!redisHost || redisHost === '') {
    throw new Error('No redis connection provided (REDIS_HOST) ...');
  }
  if (!redisPort) {
    throw new Error('No redis connection provided (REDIS_PORT) ...');
  }
  return { host: redisHost, port: redisPort };
};

export const getNewRedisClient = (config: RedisConfig = getDefaultRedisConfigs()): IRedisClient => {
  if (config instanceof Array) {
    return new Redis.Cluster(config) as unknown as IRedisClient;
  }
  return new Redis(config) as unknown as IRedisClient;
};

// Legacy export kept for backwards compatibility
export const getExistingRedisClient = (() => {
  let _client: IRedisClient | undefined;
  return (config: RedisConfig = getDefaultRedisConfigs()): IRedisClient => {
    if (!_client) _client = getNewRedisClient(config);
    return _client;
  };
})();
