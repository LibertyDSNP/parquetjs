import * as rle from './rle';
import { Cursor, Options } from './types';

// Union type for all possible decoded values from PLAIN_DICTIONARY codec
export type PlainDictionaryDecodedValue = number; // Dictionary indices are always numbers

export const encodeValues = function (type: string, values: number[], opts: Options): Buffer {
  if (!opts.bitWidth) {
    throw new Error('bitWidth is required for PLAIN_DICTIONARY encoding');
  }
  
  // PLAIN_DICTIONARY format: bit width (1 byte) + RLE encoded indices
  const bitWidthBuffer = Buffer.from([opts.bitWidth]);
  const rleBuffer = rle.encodeValues(type, values, Object.assign({}, opts, { disableEnvelope: true }));
  
  return Buffer.concat([bitWidthBuffer, rleBuffer]);
};

export const decodeValues = function (type: string, cursor: Cursor, count: number, opts: Options): number[] {
  const bitWidth = cursor.buffer.subarray(cursor.offset, cursor.offset + 1).readInt8(0);
  cursor.offset += 1;
  return rle.decodeValues(type, cursor, count, Object.assign({}, opts, { disableEnvelope: true, bitWidth }));
};
