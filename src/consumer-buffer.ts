export type ConsumerBufferConfigs = {
  ack: (...ids: string[]) => Promise<void>;
  process: (id: string, message: string[]) => Promise<any>;
  error: (id: string, message: string[], error: Error) => Promise<void>;
  mode: 'parallel' | 'serial';
  size: number;
};

export class ConsumerBuffer {

  private buffer: [string, string[]][] = [];
  private isDoing = false;

  constructor(private readonly configs: ConsumerBufferConfigs) {}

  get size(): number {
    return this.configs.size;
  }

  async add(...rawMessages: [string, string[]][]): Promise<void> {
    this.buffer.push(...rawMessages);
    return this.do();
  }

  private async do(): Promise<void> {
    if (this.isDoing) return;
    this.isDoing = true;

    const toBeAck: string[] = [];
    const operations: Promise<void>[] = [];

    while (this.buffer.length) {
      const [id, message] = this.buffer.shift()!;

      if (this.configs.mode === 'serial') {
        const succeeded = await this.configs.process(id, message)
          .then(() => true)
          .catch(async err => {
            await this.configs.error(id, message, err).catch(() => {});
            return false;
          });
        if (succeeded) await this.configs.ack(id);
      } else {
        const operation = this.configs.process(id, message)
          .then(() => { toBeAck.push(id); })
          .catch(err => this.configs.error(id, message, err).catch(() => {}));
        operations.push(operation);
      }
    }

    this.isDoing = false;

    if (this.configs.mode === 'parallel') {
      await Promise.all(operations);
      if (toBeAck.length) {
        await this.configs.ack(...toBeAck);
      }
    }
  }
}
