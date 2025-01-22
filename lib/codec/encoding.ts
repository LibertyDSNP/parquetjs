///
/// Modified under the MIT License from
/// https://github.com/hyparam/hyparquet
/// The MIT License (MIT)
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
import {ParquetType} from "../declare";
import {DataReader, DecodedArray } from "./types";

/**
 * Var int, also known as Unsigned LEB128.
 * Var ints take 1 to 5 bytes (int32) or 1 to 10 bytes (int64).
 * Reads groups of 7 low bits until high bit is 0.
 *
 * @param {DataReader} reader
 * @returns {number} value
 */
export function readVarInt(reader: DataReader) {
  let result = 0
  let shift = 0
  while (true) {
    const byte = reader.view.getUint8(reader.offset++)
    result |= (byte & 0x7f) << shift
    if (!(byte & 0x80)) {
      return result
    }
    shift += 7
  }
}

/**
 * Minimum bits needed to store value.
 *
 * @param {number} value
 * @returns {number}
 */
export function bitWidth(value: number): number {
  return 32 - Math.clz32(value)
}

/**
 * Read values from a run-length encoded/bit-packed hybrid encoding.
 *
 * If length is zero, then read int32 length at the start.
 *
 * @typedef {import("./types.d.ts").DataReader} DataReader
 * @typedef {import("./types.d.ts").DecodedArray} DecodedArray
 * @param {DataReader} reader
 * @param {number} width - width of each bit-packed group
 * @param {number} length - length of the encoded data, in bytes (?)
 * @param {DecodedArray} output
 * @param {disableEnvelope} - set to true to consume entire buffer, false to assume (and therefore skip) a 4 byte header
 */
export function readRleBitPackedHybrid(reader: DataReader, width: number, length: number, output: DecodedArray, disableEnvelope?: boolean) {

  if (!disableEnvelope) {
    reader.offset += 4
  }
  const startOffset = reader.offset;
  let seen = 0
  while (seen < output.length) {
    const header = readVarInt(reader)
    if (header & 1) {
      // bit-packed
      seen = readBitPacked(reader, header, width, output, seen)
    } else {
      // rle
      const count = header >>> 1
      readRle(reader, count, width, output, seen)
      seen += count
    }
  }
  console.assert(reader.offset - startOffset === length)
}

/**
 * Run-length encoding: read value with bitWidth and repeat it count times.
 *
 * @param {DataReader} reader
 * @param {number} count
 * @param {number} bitWidth
 * @param {DecodedArray} output
 * @param {number} seen
 */
export function readRle(reader: DataReader,
                        count: number,
                        bitWidth: number,
                        output: DecodedArray,
                        seen: number) {
  const width = bitWidth + 7 >> 3
  let value = 0
  for (let i = 0; i < width; i++) {
    value |= reader.view.getUint8(reader.offset++) << (i << 3)
  }
  // assert(value < 1 << bitWidth)

  // repeat value count times
  for (let i = 0; i < count; i++) {
    output[seen + i] = value
  }
}

/**
 * Read a bit-packed run of the rle/bitpack hybrid.
 * Supports width > 8 (crossing bytes).
 *
 * @param {DataReader} reader
 * @param {number} header - bit-pack header
 * @param {number} bitWidth
 * @param {DecodedArray} output
 * @param {number} seen
 * @returns {number} total output values so far
 */
export function readBitPacked(reader: DataReader,
                              header: number,
                              bitWidth: number,
                              output: DecodedArray,
                              seen: number): number {
  let count = header >> 1 << 3 // values to read
  const mask = (1 << bitWidth) - 1

  let data = 0
  if (reader.offset < reader.view.byteLength) {
    data = reader.view.getUint8(reader.offset++)
  } else if (mask) {
    // sometimes out-of-bounds reads are masked out
    throw new Error(`parquet bitpack offset ${reader.offset} out of range`)
  }
  let left = 8
  let right = 0

  // read values
  while (count) {
    // if we have crossed a byte boundary, shift the data
    if (right > 8) {
      right -= 8
      left -= 8
      data >>>= 8
    } else if (left - right < bitWidth) {
      // if we don't have bitWidth number of bits to read, read next byte
      data |= reader.view.getUint8(reader.offset) << left
      reader.offset++
      left += 8
    } else {
      if (seen < output.length) {
        // emit value
        output[seen++] = data >> right & mask
      }
      count--
      right += bitWidth
    }
  }

  return seen
}

/**
 * @typedef {import("./types.d.ts").ParquetType} ParquetType
 * @param {DataReader} reader
 * @param {number} count
 * @param {ParquetType} type
 * @param {number | undefined} typeLength
 * @returns {DecodedArray}
 */
export function byteStreamSplit(reader: DataReader, count: number, type: ParquetType, typeLength: number|undefined) {
  const width = byteWidth(type, typeLength)
  const bytes = new Uint8Array(count * width)
  for (let b = 0; b < width; b++) {
    for (let i = 0; i < count; i++) {
      bytes[i * width + b] = reader.view.getUint8(reader.offset++)
    }
  }
  // interpret bytes as typed array
  if (type === 'FLOAT') return new Float32Array(bytes.buffer)
  else if (type === 'DOUBLE') return new Float64Array(bytes.buffer)
  else if (type === 'INT32') return new Int32Array(bytes.buffer)
  else if (type === 'INT64') return new BigInt64Array(bytes.buffer)
  else if (type === 'FIXED_LEN_BYTE_ARRAY') {
    // split into arrays of typeLength
    const split = new Array(count)
    for (let i = 0; i < count; i++) {
      split[i] = bytes.subarray(i * width, (i + 1) * width)
    }
    return split
  }
  throw new Error(`parquet byte_stream_split unsupported type: ${type}`)
}

/**
 * @param {ParquetType} type
 * @param {number | undefined} typeLength
 * @returns {number}
 */
function byteWidth(type: ParquetType, typeLength: number|undefined): number {
  switch (type) {
    case 'INT32':
    case 'FLOAT':
      return 4
    case 'INT64':
    case 'DOUBLE':
      return 8
    case 'FIXED_LEN_BYTE_ARRAY':
      if (!typeLength) throw new Error('parquet byteWidth missing type_length')
      return typeLength
    default:
      throw new Error(`parquet unsupported type: ${type}`)
  }
}
