import * as rle from './rle';
import { Cursor, Options } from './types';

// Union type for all possible decoded values from RLE_DICTIONARY codec
export type RleDictionaryDecodedValue = number; // Dictionary indices are always numbers

export const encodeValues = function (type: string, values: number[], opts: Options): Buffer {
  if (!opts.bitWidth) {
    throw new Error('bitWidth is required for RLE_DICTIONARY encoding');
  }
  
  // RLE_DICTIONARY format: pure RLE encoded indices (no bit width header)
  return rle.encodeValues(type, values, Object.assign({}, opts, { disableEnvelope: true }));
};

export const decodeValues = function (type: string, cursor: Cursor, count: number, opts: Options): number[] {
  // RLE_DICTIONARY uses pure RLE decoding (bit width should already be in opts)
  return rle.decodeValues(type, cursor, count, Object.assign({}, opts, { disableEnvelope: true }));
};