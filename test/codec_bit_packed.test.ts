import { expect } from 'chai';
import * as parquet_codec_bit_packed from '../lib/codec/bit_packed';

describe('ParquetCodec::BIT_PACKED', function () {
  it('should encode and decode spec example: numbers 1-7 with bit width 3', function () {
    // Spec example: numbers 1 through 7 using bit width 3
    // dec value: 0   1   2   3   4   5   6   7
    // bit value: 000 001 010 011 100 101 110 111
    // bit label: ABC DEF GHI JKL MNO PQR STU VWX
    // Expected: 00000101 00111001 01110111 (MSB first packing)
    const values = [0, 1, 2, 3, 4, 5, 6, 7];
    const opts = { bitWidth: 3 };
    
    const encoded = parquet_codec_bit_packed.encodeValues('REPETITION_LEVELS', values, opts);
    
    // Verify the exact byte pattern from spec (MSB first)
    const expected = Buffer.from([0x05, 0x39, 0x77]); // 00000101 00111001 01110111
    expect(encoded).to.deep.equal(expected);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_bit_packed.decodeValues('REPETITION_LEVELS', cursor, values.length, opts);
    
    expect(decoded).to.deep.equal(values);
  });

  it('should encode and decode repetition levels with bit width 2', function () {
    // Test with bit width 2 (max repetition level of 3)
    const values = [0, 1, 2, 3, 0, 1, 2, 3];
    const opts = { bitWidth: 2 };
    
    const encoded = parquet_codec_bit_packed.encodeValues('REPETITION_LEVELS', values, opts);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_bit_packed.decodeValues('REPETITION_LEVELS', cursor, values.length, opts);
    
    expect(decoded).to.deep.equal(values);
  });

  it('should encode and decode definition levels with bit width 2', function () {
    // Test with definition levels
    const values = [2, 3, 3, 2, 1, 0, 1, 2];
    const opts = { bitWidth: 2 };
    
    const encoded = parquet_codec_bit_packed.encodeValues('DEFINITION_LEVELS', values, opts);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_bit_packed.decodeValues('DEFINITION_LEVELS', cursor, values.length, opts);
    
    expect(decoded).to.deep.equal(values);
  });

  it('should handle bit width 1 (boolean-like)', function () {
    const values = [0, 1, 1, 0, 1, 0, 0, 1];
    const opts = { bitWidth: 1 };
    
    const encoded = parquet_codec_bit_packed.encodeValues('REPETITION_LEVELS', values, opts);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_bit_packed.decodeValues('REPETITION_LEVELS', cursor, values.length, opts);
    
    expect(decoded).to.deep.equal(values);
  });

  it('should handle bit width 4', function () {
    const values = [0, 1, 2, 15, 8, 7, 3, 12];
    const opts = { bitWidth: 4 };
    
    const encoded = parquet_codec_bit_packed.encodeValues('REPETITION_LEVELS', values, opts);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_bit_packed.decodeValues('REPETITION_LEVELS', cursor, values.length, opts);
    
    expect(decoded).to.deep.equal(values);
  });

  it('should handle non-byte-aligned lengths', function () {
    // 30 values with 2 bits each = 60 bits = 8 bytes (last byte padded)
    const values = Array.from({ length: 30 }, (_, i) => i % 4);
    const opts = { bitWidth: 2 };
    
    const encoded = parquet_codec_bit_packed.encodeValues('REPETITION_LEVELS', values, opts);
    expect(encoded.length).to.equal(8); // 30 * 2 bits = 60 bits = 8 bytes
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_bit_packed.decodeValues('REPETITION_LEVELS', cursor, values.length, opts);
    
    expect(decoded).to.deep.equal(values);
  });

  it('should handle empty array', function () {
    const values: number[] = [];
    const opts = { bitWidth: 3 };
    
    const encoded = parquet_codec_bit_packed.encodeValues('REPETITION_LEVELS', values, opts);
    expect(encoded.length).to.equal(0);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_bit_packed.decodeValues('REPETITION_LEVELS', cursor, values.length, opts);
    
    expect(decoded).to.deep.equal([]);
  });

  it('should handle single value', function () {
    const values = [5];
    const opts = { bitWidth: 3 };
    
    const encoded = parquet_codec_bit_packed.encodeValues('REPETITION_LEVELS', values, opts);
    expect(encoded.length).to.equal(1); // 1 * 3 bits = 3 bits = 1 byte
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_bit_packed.decodeValues('REPETITION_LEVELS', cursor, values.length, opts);
    
    expect(decoded).to.deep.equal(values);
  });

  it('should throw error when bitWidth is missing', function () {
    expect(function() {
      parquet_codec_bit_packed.encodeValues('REPETITION_LEVELS', [1, 2, 3], {});
    }).to.throw('bitWidth is required for BIT_PACKED encoding');
    
    expect(function() {
      parquet_codec_bit_packed.decodeValues('REPETITION_LEVELS', { buffer: Buffer.alloc(10), offset: 0 }, 3, {});
    }).to.throw('bitWidth is required for BIT_PACKED decoding');
  });

  it('should handle padding correctly', function () {
    // Test that padding bits are handled correctly (should be 0 but readers must accept any value)
    const values = [7, 6, 5]; // 3 values with 3 bits each = 9 bits, needs 2 bytes (7 padding bits)
    const opts = { bitWidth: 3 };
    
    const encoded = parquet_codec_bit_packed.encodeValues('REPETITION_LEVELS', values, opts);
    expect(encoded.length).to.equal(2); // 9 bits = 2 bytes
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_bit_packed.decodeValues('REPETITION_LEVELS', cursor, values.length, opts);
    
    expect(decoded).to.deep.equal(values);
  });

  it('should verify MSB-first bit packing order', function () {
    // Verify the deprecated bit packing order (MSB first) is different from RLE hybrid
    const values = [0, 1]; // 00 01 with bit width 2
    const opts = { bitWidth: 2 };
    
    const encoded = parquet_codec_bit_packed.encodeValues('REPETITION_LEVELS', values, opts);
    
    // In MSB-first: bits are packed as 00010000 (0x10)
    // In LSB-first (RLE hybrid): bits would be packed as 01000000 (0x40)
    expect(encoded[0]).to.equal(0x10); // Verify MSB-first order
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_bit_packed.decodeValues('REPETITION_LEVELS', cursor, values.length, opts);
    
    expect(decoded).to.deep.equal(values);
  });
});