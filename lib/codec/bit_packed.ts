import { Cursor, Options } from './types';

export type BitPackedDecodedValue = number;

export const encodeValues = function (
  type: string,
  values: number[],
  opts: Options
): Buffer {
  // BIT_PACKED is only used for repetition and definition levels
  if (!opts.bitWidth) {
    throw new Error('bitWidth is required for BIT_PACKED encoding');
  }
  
  if (values.length === 0) {
    return Buffer.alloc(0);
  }
  
  const bitWidth = opts.bitWidth;
  const totalBits = values.length * bitWidth;
  const totalBytes = Math.ceil(totalBits / 8);
  const buf = Buffer.alloc(totalBytes);
  buf.fill(0);
  
  // Pack values from most significant bit to least significant bit
  // This is the deprecated bit packing order, different from RLE hybrid
  for (let i = 0; i < values.length; i++) {
    const value = values[i];
    const startBit = i * bitWidth;
    
    // Pack bits in MSB to LSB order within each byte
    for (let bit = 0; bit < bitWidth; bit++) {
      if (value & (1 << (bitWidth - 1 - bit))) {
        const globalBit = startBit + bit;
        const byteIndex = Math.floor(globalBit / 8);
        const bitIndex = 7 - (globalBit % 8); // MSB first within byte
        buf[byteIndex] |= (1 << bitIndex);
      }
    }
  }
  
  return buf;
};

export const decodeValues = function (
  type: string,
  cursor: Cursor,
  count: number,
  opts: Options
): number[] {
  // BIT_PACKED is only used for repetition and definition levels
  if (!opts.bitWidth) {
    throw new Error('bitWidth is required for BIT_PACKED decoding');
  }
  
  if (count === 0) {
    return [];
  }
  
  const bitWidth = opts.bitWidth;
  const totalBits = count * bitWidth;
  const totalBytes = Math.ceil(totalBits / 8);
  
  // Ensure we have enough data
  if (cursor.buffer.length - cursor.offset < totalBytes) {
    throw new Error(`Not enough data: need ${totalBytes} bytes, have ${cursor.buffer.length - cursor.offset}`);
  }
  
  const values: number[] = [];
  
  // Unpack values using MSB to LSB order (deprecated bit packing)
  for (let i = 0; i < count; i++) {
    let value = 0;
    const startBit = i * bitWidth;
    
    // Unpack bits in MSB to LSB order within each byte
    for (let bit = 0; bit < bitWidth; bit++) {
      const globalBit = startBit + bit;
      const byteIndex = Math.floor(globalBit / 8);
      const bitIndex = 7 - (globalBit % 8); // MSB first within byte
      
      if (cursor.buffer[cursor.offset + byteIndex] & (1 << bitIndex)) {
        value |= (1 << (bitWidth - 1 - bit));
      }
    }
    
    values.push(value);
  }
  
  cursor.offset += totalBytes;
  return values;
};