import * as deltaBinaryPacked from './delta_binary_packed';
import * as deltaLengthByteArray from './delta_length_byte_array';
import { Cursor, Options } from './types';

export type DeltaByteArrayDecodedValue = Buffer;

export const encodeValues = function (
  type: string,
  values: (Buffer | Uint8Array | string)[],
  opts: Options
): Buffer {
  if (type !== 'BYTE_ARRAY' && type !== 'FIXED_LEN_BYTE_ARRAY') {
    throw new Error('DELTA_BYTE_ARRAY only supports BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY types');
  }
  
  if (values.length === 0) {
    return Buffer.alloc(0);
  }
  
  // Convert all values to Buffers
  const bufferValues: Buffer[] = [];
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
  }
  
  // Compute prefix lengths and suffixes using incremental encoding
  const prefixLengths: number[] = [];
  const suffixes: Buffer[] = [];
  
  for (let i = 0; i < bufferValues.length; i++) {
    const current = bufferValues[i];
    
    if (i === 0) {
      // First element has no prefix
      prefixLengths.push(0);
      suffixes.push(current);
    } else {
      const previous = bufferValues[i - 1];
      
      // Find common prefix length
      let prefixLength = 0;
      const minLength = Math.min(current.length, previous.length);
      
      for (let j = 0; j < minLength; j++) {
        if (current[j] === previous[j]) {
          prefixLength++;
        } else {
          break;
        }
      }
      
      prefixLengths.push(prefixLength);
      
      // Suffix is the part after the common prefix
      const suffix = current.subarray(prefixLength);
      suffixes.push(suffix);
    }
  }
  
  // Encode prefix lengths using DELTA_BINARY_PACKED
  const prefixLengthsBuffer = deltaBinaryPacked.encodeValues('INT32', prefixLengths, opts);
  
  // Encode suffixes using DELTA_LENGTH_BYTE_ARRAY
  const suffixesBuffer = deltaLengthByteArray.encodeValues('BYTE_ARRAY', suffixes, opts);
  
  // Return: <Delta Encoded Prefix Lengths> <Delta Length Byte Array Suffixes>
  return Buffer.concat([prefixLengthsBuffer, suffixesBuffer]);
};

export const decodeValues = function (
  type: string,
  cursor: Cursor,
  count: number,
  opts: Options
): Buffer[] {
  if (type !== 'BYTE_ARRAY' && type !== 'FIXED_LEN_BYTE_ARRAY') {
    throw new Error('DELTA_BYTE_ARRAY only supports BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY types');
  }
  
  if (count === 0) {
    return [];
  }
  
  // Decode prefix lengths using DELTA_BINARY_PACKED
  const prefixLengths = deltaBinaryPacked.decodeValues('INT32', cursor, count, opts);
  
  // Decode suffixes using DELTA_LENGTH_BYTE_ARRAY
  const suffixes = deltaLengthByteArray.decodeValues('BYTE_ARRAY', cursor, count, opts);
  
  // Reconstruct original strings using incremental decoding
  const values: Buffer[] = [];
  let previousValue = Buffer.alloc(0);
  
  for (let i = 0; i < count; i++) {
    const prefixLength = prefixLengths[i];
    const suffix = suffixes[i];
    
    if (prefixLength < 0) {
      throw new Error(`Invalid negative prefix length: ${prefixLength}`);
    }
    
    if (prefixLength > previousValue.length) {
      throw new Error(`Prefix length ${prefixLength} exceeds previous value length ${previousValue.length}`);
    }
    
    // Reconstruct current value: prefix from previous + suffix
    const prefix = previousValue.subarray(0, prefixLength);
    const currentValue = Buffer.concat([prefix, suffix]);
    
    values.push(currentValue);
    previousValue = currentValue;
  }
  
  return values;
};