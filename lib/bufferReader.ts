import { Statistics } from '../gen-nodejs/parquet_types';
import { ParquetEnvelopeReader } from './reader';
import { FileMetaDataExt } from './declare';

export interface BufferReaderOptions {
  maxSpan?: number;
  maxLength?: number;
  queueWait?: number;
  default_dictionary_size?: number;
  metadata?: FileMetaDataExt;
  rawStatistics?: Statistics;
}

interface BufferReaderQueueRow {
  offset: number;
  length: number;
  resolve: (buf: Buffer) => void;
  reject: unknown;
}

export default class BufferReader {
  maxSpan: number;
  maxLength: number;
  queueWait: number;
  scheduled?: boolean;
  queue: BufferReaderQueueRow[];
  envelopeReader: ParquetEnvelopeReader;

  constructor(envelopeReader: ParquetEnvelopeReader, options: BufferReaderOptions) {
    options = options || {};
    this.envelopeReader = envelopeReader;
    this.maxSpan = options.maxSpan || 100000; // 100k
    this.maxLength = options.maxLength || 10000000; // 10mb
    this.queueWait = options.queueWait || 5;
    this.scheduled = undefined;
    this.queue = [];
  }

  read(offset: number, length: number): Promise<Buffer> {
    if (!this.scheduled) {
      this.scheduled = true;
      setTimeout(() => {
        this.scheduled = false;
        this.processQueue();
      }, this.queueWait);
    }

    return new Promise((resolve, reject) => {
      this.queue.push({ offset, length, resolve, reject });
    });
  }

  async processQueue() {
    const queue = this.queue;
    if (!queue.length) return;
    this.queue = [];
    queue.sort((a, b) => a.offset - b.offset);

    let subqueue: BufferReaderQueueRow[] = [];

    const readSubqueue = async () => {
      if (!subqueue.length) {
        return;
      }

      const processQueue = subqueue;
      subqueue = [];

      const lastElement = processQueue[processQueue.length - 1];
      const start = processQueue[0].offset;
      const finish = lastElement.offset + lastElement.length;
      const buffer = await this.envelopeReader.readFn(start, finish - start);

      processQueue.forEach(async (d) => {
        d.resolve(buffer.subarray(d.offset - start, d.offset + d.length - start));
      });
    };

    queue.forEach((d, i) => {
      const prev = queue[i - 1];
      if (!prev || d.offset - (prev.offset + prev.length) < this.maxSpan) {
        subqueue.push(d);
        if (d.offset + d.length - subqueue[0].offset > this.maxLength) {
          readSubqueue();
        }
      } else {
        readSubqueue();
        subqueue = [d];
      }
    });
    readSubqueue();
  }
}
