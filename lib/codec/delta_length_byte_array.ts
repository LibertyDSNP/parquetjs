import * as deltaBinaryPacked from './delta_binary_packed';
import { Cursor, Options } from './types';

export type DeltaLengthByteArrayDecodedValue = Buffer;

export const encodeValues = function (
  type: string,
  values: (Buffer | Uint8Array | string)[],
  opts: Options
): Buffer {
  if (type !== 'BYTE_ARRAY') {
    throw new Error('DELTA_LENGTH_BYTE_ARRAY only supports BYTE_ARRAY type');
  }
  
  if (values.length === 0) {
    return Buffer.alloc(0);
  }
  
  // Convert all values to Buffers and extract lengths
  const bufferValues: Buffer[] = [];
  const lengths: number[] = [];
  
  for (const value of values) {
    let buffer: Buffer;
    if (Buffer.isBuffer(value)) {
      buffer = value;
    } else if (value instanceof Uint8Array) {
      buffer = Buffer.from(value);
    } else if (typeof value === 'string') {
      buffer = Buffer.from(value, 'utf8');
    } else {
      throw new Error('Invalid value type for BYTE_ARRAY');
    }
    
    bufferValues.push(buffer);
    lengths.push(buffer.length);
  }
  
  // Encode lengths using DELTA_BINARY_PACKED
  const lengthsBuffer = deltaBinaryPacked.encodeValues('INT32', lengths, opts);
  
  // Concatenate all byte array data
  const dataBuffer = Buffer.concat(bufferValues);
  
  // Return: <Delta Encoded Lengths> <Byte Array Data>
  return Buffer.concat([lengthsBuffer, dataBuffer]);
};

export const decodeValues = function (
  type: string,
  cursor: Cursor,
  count: number,
  opts: Options
): Buffer[] {
  if (type !== 'BYTE_ARRAY') {
    throw new Error('DELTA_LENGTH_BYTE_ARRAY only supports BYTE_ARRAY type');
  }
  
  if (count === 0) {
    return [];
  }
  
  const startOffset = cursor.offset;
  
  // Decode lengths using DELTA_BINARY_PACKED
  const lengths = deltaBinaryPacked.decodeValues('INT32', cursor, count, opts);
  
  // Now cursor is positioned at the start of concatenated byte array data
  const values: Buffer[] = [];
  
  for (let i = 0; i < count; i++) {
    const length = lengths[i];
    if (length < 0) {
      throw new Error(`Invalid negative length: ${length}`);
    }
    
    const value = cursor.buffer.subarray(cursor.offset, cursor.offset + length);
    values.push(value);
    cursor.offset += length;
  }
  
  return values;
};