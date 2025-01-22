import * as rle from './rle';
import {Cursor, DataReader, DecodedArray, Options} from './types'
import {readRleBitPackedHybrid} from "./encoding";

export const decodeValues = function (type: string, cursor: Cursor, count: number, opts: Options) {
  const bitWidth = cursor.buffer.subarray(cursor.offset, cursor.offset + 1).readInt8(0);
  cursor.offset += 1;
  // old:
  //   return rle.decodeValues(type, cursor, count, Object.assign({}, opts, { disableEnvelope: true, bitWidth }));
  const reader: DataReader = {
    view: new DataView(cursor.buffer.buffer, cursor.offset),
    offset: 0,
  }
  let output: DecodedArray = new Array(count);
  const disableEnvelope = true;
  readRleBitPackedHybrid(reader, bitWidth, count, output, disableEnvelope)
  return output;
};
