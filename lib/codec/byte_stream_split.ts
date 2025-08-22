import { Cursor, Options } from './types';

export type ByteStreamSplitDecodedValue = number | Buffer;

function getTypeSize(type: string): number {
  switch (type) {
    case 'FLOAT':
      return 4;
    case 'DOUBLE':
      return 8;
    case 'INT32':
      return 4;
    case 'INT64':
      return 8;
    case 'FIXED_LEN_BYTE_ARRAY':
      return 0; // Will be determined from opts.typeLength
    default:
      throw new Error(`BYTE_STREAM_SPLIT does not support type: ${type}`);
  }
}

export const encodeValues = function (
  type: string,
  values: (number | Buffer | Uint8Array)[],
  opts: Options
): Buffer {
  if (!['FLOAT', 'DOUBLE', 'INT32', 'INT64', 'FIXED_LEN_BYTE_ARRAY'].includes(type)) {
    throw new Error('BYTE_STREAM_SPLIT only supports FLOAT, DOUBLE, INT32, INT64, FIXED_LEN_BYTE_ARRAY types');
  }
  
  if (values.length === 0) {
    return Buffer.alloc(0);
  }
  
  let typeSize = getTypeSize(type);
  if (type === 'FIXED_LEN_BYTE_ARRAY') {
    if (!opts.typeLength) {
      throw new Error('typeLength is required for FIXED_LEN_BYTE_ARRAY');
    }
    typeSize = opts.typeLength;
  }
  
  const N = values.length;
  const K = typeSize;
  
  // Convert all values to byte arrays
  const valueBuffers: Buffer[] = [];
  
  for (const value of values) {
    let buffer: Buffer;
    
    if (Buffer.isBuffer(value)) {
      buffer = value;
    } else if (value instanceof Uint8Array) {
      buffer = Buffer.from(value);
    } else if (typeof value === 'number') {
      buffer = Buffer.alloc(K);
      switch (type) {
        case 'FLOAT':
          buffer.writeFloatLE(value, 0);
          break;
        case 'DOUBLE':
          buffer.writeDoubleLE(value, 0);
          break;
        case 'INT32':
          buffer.writeInt32LE(value, 0);
          break;
        case 'INT64':
          buffer.writeBigInt64LE(BigInt(value), 0);
          break;
        default:
          throw new Error(`Cannot convert number to ${type}`);
      }
    } else {
      throw new Error(`Invalid value type for ${type}`);
    }
    
    if (buffer.length !== K) {
      throw new Error(`Value size ${buffer.length} does not match expected size ${K} for type ${type}`);
    }
    
    valueBuffers.push(buffer);
  }
  
  // Create K streams of length N
  const streams: Buffer[] = [];
  for (let k = 0; k < K; k++) {
    streams.push(Buffer.alloc(N));
  }
  
  // Scatter bytes to streams
  for (let n = 0; n < N; n++) {
    const valueBuffer = valueBuffers[n];
    for (let k = 0; k < K; k++) {
      streams[k][n] = valueBuffer[k];
    }
  }
  
  // Concatenate streams in order: 0-th stream, 1-st stream, etc.
  return Buffer.concat(streams);
};

export const decodeValues = function (
  type: string,
  cursor: Cursor,
  count: number,
  opts: Options
): (number | Buffer)[] {
  if (!['FLOAT', 'DOUBLE', 'INT32', 'INT64', 'FIXED_LEN_BYTE_ARRAY'].includes(type)) {
    throw new Error('BYTE_STREAM_SPLIT only supports FLOAT, DOUBLE, INT32, INT64, FIXED_LEN_BYTE_ARRAY types');
  }
  
  if (count === 0) {
    return [];
  }
  
  let typeSize = getTypeSize(type);
  if (type === 'FIXED_LEN_BYTE_ARRAY') {
    const configuredTypeLength = opts.typeLength ?? (opts.column ? opts.column.typeLength : undefined);
    if (!configuredTypeLength) {
      throw new Error('typeLength is required for FIXED_LEN_BYTE_ARRAY');
    }
    typeSize = configuredTypeLength;
  }
  
  const N = count;
  const K = typeSize;
  const totalBytes = K * N;
  
  // Ensure we have enough data
  if (cursor.buffer.length - cursor.offset < totalBytes) {
    throw new Error(`Not enough data: need ${totalBytes} bytes, have ${cursor.buffer.length - cursor.offset}`);
  }
  
  // Read K streams of length N
  const streams: Buffer[] = [];
  for (let k = 0; k < K; k++) {
    streams.push(cursor.buffer.subarray(cursor.offset + k * N, cursor.offset + (k + 1) * N));
  }
  cursor.offset += totalBytes;
  
  // Reconstruct values by gathering bytes from streams
  const values: (number | Buffer)[] = [];
  
  for (let n = 0; n < N; n++) {
    const valueBuffer = Buffer.alloc(K);
    
    // Gather bytes from each stream
    for (let k = 0; k < K; k++) {
      valueBuffer[k] = streams[k][n];
    }
    
    // Convert buffer back to appropriate type
    let decodedValue: number | Buffer;
    
    switch (type) {
      case 'FLOAT':
        decodedValue = valueBuffer.readFloatLE(0);
        break;
      case 'DOUBLE':
        decodedValue = valueBuffer.readDoubleLE(0);
        break;
      case 'INT32':
        decodedValue = valueBuffer.readInt32LE(0);
        break;
      case 'INT64':
        decodedValue = Number(valueBuffer.readBigInt64LE(0));
        break;
      case 'FIXED_LEN_BYTE_ARRAY':
        decodedValue = valueBuffer;
        break;
      default:
        throw new Error(`Unsupported type: ${type}`);
    }
    
    values.push(decodedValue);
  }
  
  return values;
};