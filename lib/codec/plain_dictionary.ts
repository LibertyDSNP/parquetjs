import * as rle from './rle';
import { Cursor, Options } from './types';

// Union type for all possible decoded values from PLAIN_DICTIONARY codec
export type PlainDictionaryDecodedValue = number; // Dictionary indices are always numbers

export const decodeValues = function (type: string, cursor: Cursor, count: number, opts: Options): PlainDictionaryDecodedValue[] {
  const bitWidth = cursor.buffer.subarray(cursor.offset, cursor.offset + 1).readInt8(0);
  cursor.offset += 1;
  return rle.decodeValues(type, cursor, count, Object.assign({}, opts, { disableEnvelope: true, bitWidth }));
};
