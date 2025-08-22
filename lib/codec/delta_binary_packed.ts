import varint from 'varint';
import { Cursor, Options } from './types';

export type DeltaBinaryPackedDecodedValue = number;

function zigzagEncode(value: number): number {
  return (value << 1) ^ (value >> 31);
}

function zigzagDecode(value: number): number {
  return (value >>> 1) ^ (-(value & 1));
}

function bitPack(values: number[], bitWidth: number): Buffer {
  if (values.length === 0) return Buffer.alloc(0);
  
  const totalBits = values.length * bitWidth;
  const buf = Buffer.alloc(Math.ceil(totalBits / 8));
  buf.fill(0);
  
  for (let i = 0; i < values.length; i++) {
    const value = values[i];
    const startBit = i * bitWidth;
    
    for (let bit = 0; bit < bitWidth; bit++) {
      if (value & (1 << bit)) {
        const globalBit = startBit + bit;
        const byteIndex = Math.floor(globalBit / 8);
        const bitIndex = globalBit % 8;
        buf[byteIndex] |= (1 << bitIndex);
      }
    }
  }
  
  return buf;
}

function bitUnpack(buffer: Buffer, offset: number, count: number, bitWidth: number): number[] {
  const values: number[] = [];
  
  for (let i = 0; i < count; i++) {
    let value = 0;
    const startBit = i * bitWidth;
    
    for (let bit = 0; bit < bitWidth; bit++) {
      const globalBit = startBit + bit;
      const byteIndex = Math.floor(globalBit / 8);
      const bitIndex = globalBit % 8;
      
      if (buffer[offset + byteIndex] & (1 << bitIndex)) {
        value |= (1 << bit);
      }
    }
    
    values.push(value);
  }
  
  return values;
}

export const encodeValues = function (
  type: string,
  values: number[],
  opts: Options
): Buffer {
  if (type !== 'INT32' && type !== 'INT64') {
    throw new Error('DELTA_BINARY_PACKED only supports INT32 and INT64 types');
  }
  
  if (values.length === 0) {
    return Buffer.alloc(0);
  }
  
  // Default block size (must be multiple of 128)
  const blockSize = Math.max(128, Math.ceil(values.length / 128) * 128);
  // Default miniblock count (divisor of block size, quotient must be multiple of 32)
  const miniblockCount = 4;
  const valuesPerMiniblock = blockSize / miniblockCount;
  
  if (valuesPerMiniblock % 32 !== 0) {
    throw new Error('Values per miniblock must be multiple of 32');
  }
  
  const buffers: Buffer[] = [];
  
  // Header: <block size> <miniblock count> <total count> <first value>
  buffers.push(Buffer.from(varint.encode(blockSize)));
  buffers.push(Buffer.from(varint.encode(miniblockCount)));
  buffers.push(Buffer.from(varint.encode(values.length)));
  buffers.push(Buffer.from(varint.encode(zigzagEncode(values[0]))));
  
  let prevValue = values[0];
  let valueIndex = 1;
  
  while (valueIndex < values.length) {
    const blockValues = Math.min(blockSize, values.length - valueIndex);
    const deltas: number[] = [];
    
    // Compute deltas for this block
    for (let i = 0; i < blockValues; i++) {
      const delta = values[valueIndex + i] - prevValue;
      deltas.push(delta);
      prevValue = values[valueIndex + i];
    }
    
    // Find min delta (frame of reference)
    const minDelta = Math.min(...deltas);
    
    // Subtract min delta from all deltas
    const adjustedDeltas = deltas.map(d => d - minDelta);
    
    // Write min delta
    buffers.push(Buffer.from(varint.encode(zigzagEncode(minDelta))));
    
    // Process miniblocks
    const bitWidths: number[] = [];
    const miniblockBuffers: Buffer[] = [];
    
    for (let mb = 0; mb < miniblockCount; mb++) {
      const startIdx = mb * valuesPerMiniblock;
      const endIdx = Math.min(startIdx + valuesPerMiniblock, adjustedDeltas.length);
      const miniblockValues = adjustedDeltas.slice(startIdx, endIdx);
      
      // Pad miniblock to full size with zeros
      while (miniblockValues.length < valuesPerMiniblock) {
        miniblockValues.push(0);
      }
      
      // Calculate bit width needed for this miniblock
      const maxValue = Math.max(...miniblockValues, 0);
      const bitWidth = maxValue === 0 ? 0 : Math.ceil(Math.log2(maxValue + 1));
      bitWidths.push(bitWidth);
      
      if (bitWidth > 0) {
        miniblockBuffers.push(bitPack(miniblockValues, bitWidth));
      }
    }
    
    // Write bit widths
    for (const width of bitWidths) {
      buffers.push(Buffer.from([width]));
    }
    
    // Write miniblock data
    buffers.push(...miniblockBuffers);
    
    valueIndex += blockValues;
  }
  
  return Buffer.concat(buffers);
};

export const decodeValues = function (
  type: string,
  cursor: Cursor,
  count: number,
  opts: Options
): number[] {
  if (type !== 'INT32' && type !== 'INT64') {
    throw new Error('DELTA_BINARY_PACKED only supports INT32 and INT64 types');
  }
  
  if (count === 0) {
    return [];
  }
  
  // Read header
  const blockSize = varint.decode(cursor.buffer, cursor.offset);
  cursor.offset += varint.encodingLength(blockSize);
  
  const miniblockCount = varint.decode(cursor.buffer, cursor.offset);
  cursor.offset += varint.encodingLength(miniblockCount);
  
  const totalValueCount = varint.decode(cursor.buffer, cursor.offset);
  cursor.offset += varint.encodingLength(totalValueCount);
  
  const firstValue = zigzagDecode(varint.decode(cursor.buffer, cursor.offset));
  cursor.offset += varint.encodingLength(varint.decode(cursor.buffer, cursor.offset - varint.encodingLength(varint.decode(cursor.buffer, cursor.offset))));
  
  const values: number[] = [firstValue];
  const valuesPerMiniblock = blockSize / miniblockCount;
  let prevValue = firstValue;
  
  while (values.length < count) {
    // Read min delta for this block
    const minDelta = zigzagDecode(varint.decode(cursor.buffer, cursor.offset));
    cursor.offset += varint.encodingLength(varint.decode(cursor.buffer, cursor.offset - varint.encodingLength(varint.decode(cursor.buffer, cursor.offset))));
    
    // Read bit widths
    const bitWidths: number[] = [];
    for (let i = 0; i < miniblockCount; i++) {
      bitWidths.push(cursor.buffer[cursor.offset++]);
    }
    
    // Read miniblocks
    for (let mb = 0; mb < miniblockCount && values.length < count; mb++) {
      const bitWidth = bitWidths[mb];
      
      if (bitWidth === 0) {
        // All zeros
        for (let i = 0; i < valuesPerMiniblock && values.length < count; i++) {
          const delta = minDelta;
          prevValue += delta;
          values.push(prevValue);
        }
      } else {
        const packedValues = bitUnpack(cursor.buffer, cursor.offset, valuesPerMiniblock, bitWidth);
        cursor.offset += Math.ceil((valuesPerMiniblock * bitWidth) / 8);
        
        for (let i = 0; i < valuesPerMiniblock && values.length < count; i++) {
          const delta = packedValues[i] + minDelta;
          prevValue += delta;
          values.push(prevValue);
        }
      }
    }
  }
  
  return values.slice(0, count);
};