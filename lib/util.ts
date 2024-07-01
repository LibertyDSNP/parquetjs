import { TTransportCallback } from 'thrift';
import thrift from 'thrift';
import fs, { WriteStream } from 'fs';
import * as parquet_thrift from '../gen-nodejs/parquet_types';
import { FileMetaDataExt, WriterOptions } from './declare';
import { Int64 } from 'thrift';

// Use this so users only need to implement the minimal amount of the WriteStream interface
export type WriteStreamMinimal = Pick<WriteStream, 'write' | 'end'>;

/**
 * We need to patch Thrift's TFramedTransport class bc the TS type definitions
 * do not define a `readPos` field, even though the class implementation has
 * one.
 */
class fixedTFramedTransport extends thrift.TFramedTransport {
  inBuf: Buffer;
  readPos: number;
  constructor(inBuf: Buffer) {
    super(inBuf);
    this.inBuf = inBuf;
    this.readPos = 0;
  }
}

type Enums =
  | typeof parquet_thrift.Encoding
  | typeof parquet_thrift.FieldRepetitionType
  | typeof parquet_thrift.Type
  | typeof parquet_thrift.CompressionCodec
  | typeof parquet_thrift.PageType
  | typeof parquet_thrift.ConvertedType;

type ThriftObject =
  | FileMetaDataExt
  | parquet_thrift.PageHeader
  | parquet_thrift.ColumnMetaData
  | parquet_thrift.BloomFilterHeader
  | parquet_thrift.OffsetIndex
  | parquet_thrift.ColumnIndex
  | FileMetaDataExt;

/** Patch PageLocation to be three element array that has getters/setters
 * for each of the properties (offset, compressed_page_size, first_row_index)
 * This saves space considerably as we do not need to store the full variable
 * names for every PageLocation
 */

const getterSetter = (index: number) => ({
  get: function (this: number[]): number {
    return this[index];
  },
  set: function (this: number[], value: number): number {
    return (this[index] = value);
  },
});

Object.defineProperty(parquet_thrift.PageLocation.prototype, 'offset', getterSetter(0));
Object.defineProperty(parquet_thrift.PageLocation.prototype, 'compressed_page_size', getterSetter(1));
Object.defineProperty(parquet_thrift.PageLocation.prototype, 'first_row_index', getterSetter(2));

/**
 * Helper function that serializes a thrift object into a buffer
 */
export const serializeThrift = function (obj: ThriftObject) {
  const output: Uint8Array[] = [];

  const callBack: TTransportCallback = function (buf: Buffer | undefined) {
    output.push(buf as Buffer);
  };

  const transport = new thrift.TBufferedTransport(undefined, callBack);

  const protocol = new thrift.TCompactProtocol(transport);
  //@ts-expect-error, https://issues.apache.org/jira/browse/THRIFT-3872
  obj.write(protocol);
  transport.flush();

  return Buffer.concat(output);
};

export const decodeThrift = function (obj: ThriftObject, buf: Buffer, offset?: number) {
  if (!offset) {
    offset = 0;
  }

  const transport = new fixedTFramedTransport(buf);
  transport.readPos = offset;
  const protocol = new thrift.TCompactProtocol(transport);
  //@ts-expect-error, https://issues.apache.org/jira/browse/THRIFT-3872
  obj.read(protocol);
  return transport.readPos - offset;
};

/**
 * Get the number of bits required to store a given value
 */
export const getBitWidth = function (val: number) {
  if (val === 0) {
    return 0;
  } else {
    return Math.ceil(Math.log2(val + 1));
  }
};

/**
 * FIXME not ideal that this is linear
 */
export const getThriftEnum = function (klass: Enums, value: unknown) {
  for (const k in klass) {
    if (klass[k] === value) {
      return k;
    }
  }

  throw 'Invalid ENUM value';
};

export const fopen = function (filePath: string | Buffer | URL): Promise<number> {
  return new Promise((resolve, reject) => {
    fs.open(filePath, 'r', (err, fd) => {
      if (err) {
        reject(err);
      } else {
        resolve(fd);
      }
    });
  });
};

export const fstat = function (filePath: string | Buffer | URL): Promise<fs.Stats> {
  return new Promise((resolve, reject) => {
    fs.stat(filePath, (err, stat) => {
      if (err) {
        reject(err);
      } else {
        resolve(stat);
      }
    });
  });
};

export const fread = function (fd: number, position: number | null, length: number): Promise<Buffer> {
  const buffer = Buffer.alloc(length);

  return new Promise((resolve, reject) => {
    fs.read(fd, buffer, 0, length, position, (err, bytesRead, buf) => {
      if (err || bytesRead != length) {
        reject(err || Error('read failed'));
      } else {
        resolve(buf);
      }
    });
  });
};

export const fclose = function (fd: number) {
  return new Promise((resolve, reject) => {
    fs.close(fd, (err) => {
      if (err) {
        reject(err);
      } else {
        resolve(err);
      }
    });
  });
};

export const oswrite = function (os: WriteStreamMinimal, buf: Buffer) {
  return new Promise((resolve, reject) => {
    os.write(buf, (err: Error | undefined | null) => {
      if (err) {
        reject(err);
      } else {
        resolve(err);
      }
    });
  });
};

export const osend = function (os: WriteStreamMinimal) {
  return new Promise((resolve, reject) => {
    os.end((err: Error) => {
      if (err) {
        reject(err);
      } else {
        resolve(err);
      }
    });
  });
};

export const osopen = function (path: string | Buffer | URL, opts?: WriterOptions): Promise<WriteStream> {
  return new Promise((resolve, reject) => {
    const outputStream = fs.createWriteStream(path, opts);

    outputStream.on('open', function (_fd) {
      resolve(outputStream);
    });

    outputStream.on('error', function (err) {
      reject(err);
    });
  });
};

export const fieldIndexOf = function (arr: unknown[][], elem: unknown[]) {
  for (let j = 0; j < arr.length; ++j) {
    if (arr[j].length !== elem.length) {
      continue;
    }

    let m = true;
    for (let i = 0; i < elem.length; ++i) {
      if (arr[j][i] !== elem[i]) {
        m = false;
        break;
      }
    }

    if (m) {
      return j;
    }
  }

  return -1;
};

export const cloneInteger = (int: Int64) => {
  return new Int64(int.valueOf());
};
