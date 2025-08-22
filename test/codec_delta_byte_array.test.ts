import { expect } from 'chai';
import * as parquet_codec_delta_byte from '../lib/codec/delta_byte_array';

describe('ParquetCodec::DELTA_BYTE_ARRAY', function () {
  it('should encode and decode spec example: ["axis", "axle", "babble", "babyhood"]', function () {
    // Spec example: "axis", "axle", "babble", "babyhood"
    // Expected: DeltaEncoding(0, 2, 0, 3) + DeltaEncoding(4, 2, 6, 5) + "axislebabbleyhood"
    const values = ["axis", "axle", "babble", "babyhood"];
    const opts = {};
    
    const encoded = parquet_codec_delta_byte.encodeValues('BYTE_ARRAY', values, opts);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_delta_byte.decodeValues('BYTE_ARRAY', cursor, values.length, opts);
    
    expect(decoded.map(buf => buf.toString())).to.deep.equal(values);
  });

  it('should encode and decode string values with common prefixes', function () {
    const values = ["apple", "application", "apply", "applied"];
    const opts = {};
    
    const encoded = parquet_codec_delta_byte.encodeValues('BYTE_ARRAY', values, opts);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_delta_byte.decodeValues('BYTE_ARRAY', cursor, values.length, opts);
    
    expect(decoded.map(buf => buf.toString())).to.deep.equal(values);
  });

  it('should encode and decode Buffer values', function () {
    const values = [
      Buffer.from("test"),
      Buffer.from("testing"),
      Buffer.from("tester"),
      Buffer.from("tested")
    ];
    const opts = {};
    
    const encoded = parquet_codec_delta_byte.encodeValues('BYTE_ARRAY', values, opts);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_delta_byte.decodeValues('BYTE_ARRAY', cursor, values.length, opts);
    
    expect(decoded).to.deep.equal(values);
  });

  it('should encode and decode Uint8Array values', function () {
    const values = [
      new Uint8Array([0x61, 0x62]),        // "ab"
      new Uint8Array([0x61, 0x62, 0x63]),  // "abc" 
      new Uint8Array([0x61, 0x63]),        // "ac"
      new Uint8Array([0x61, 0x63, 0x64])   // "acd"
    ];
    const opts = {};
    
    const encoded = parquet_codec_delta_byte.encodeValues('BYTE_ARRAY', values, opts);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_delta_byte.decodeValues('BYTE_ARRAY', cursor, values.length, opts);
    
    // Convert Uint8Arrays to Buffers for comparison
    const expectedBuffers = values.map(arr => Buffer.from(arr));
    expect(decoded).to.deep.equal(expectedBuffers);
  });

  it('should handle values with no common prefixes', function () {
    const values = ["apple", "banana", "cherry", "date"];
    const opts = {};
    
    const encoded = parquet_codec_delta_byte.encodeValues('BYTE_ARRAY', values, opts);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_delta_byte.decodeValues('BYTE_ARRAY', cursor, values.length, opts);
    
    expect(decoded.map(buf => buf.toString())).to.deep.equal(values);
  });

  it('should handle identical values', function () {
    const values = ["same", "same", "same", "same"];
    const opts = {};
    
    const encoded = parquet_codec_delta_byte.encodeValues('BYTE_ARRAY', values, opts);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_delta_byte.decodeValues('BYTE_ARRAY', cursor, values.length, opts);
    
    expect(decoded.map(buf => buf.toString())).to.deep.equal(values);
  });

  it('should handle empty array', function () {
    const values: string[] = [];
    const opts = {};
    
    const encoded = parquet_codec_delta_byte.encodeValues('BYTE_ARRAY', values, opts);
    expect(encoded.length).to.equal(0);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_delta_byte.decodeValues('BYTE_ARRAY', cursor, values.length, opts);
    
    expect(decoded).to.deep.equal([]);
  });

  it('should handle single value', function () {
    const values = ["single"];
    const opts = {};
    
    const encoded = parquet_codec_delta_byte.encodeValues('BYTE_ARRAY', values, opts);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_delta_byte.decodeValues('BYTE_ARRAY', cursor, values.length, opts);
    
    expect(decoded.map(buf => buf.toString())).to.deep.equal(values);
  });

  it('should handle empty strings', function () {
    const values = ["", "hello", "", "world"];
    const opts = {};
    
    const encoded = parquet_codec_delta_byte.encodeValues('BYTE_ARRAY', values, opts);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_delta_byte.decodeValues('BYTE_ARRAY', cursor, values.length, opts);
    
    expect(decoded.map(buf => buf.toString())).to.deep.equal(values);
  });

  it('should support FIXED_LEN_BYTE_ARRAY type', function () {
    const values = ["abc", "abd", "abe", "abf"];
    const opts = {};
    
    const encoded = parquet_codec_delta_byte.encodeValues('FIXED_LEN_BYTE_ARRAY', values, opts);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_delta_byte.decodeValues('FIXED_LEN_BYTE_ARRAY', cursor, values.length, opts);
    
    expect(decoded.map(buf => buf.toString())).to.deep.equal(values);
  });

  it('should throw error for unsupported types', function () {
    expect(function() {
      parquet_codec_delta_byte.encodeValues('INT32', [1, 2, 3], {});
    }).to.throw('DELTA_BYTE_ARRAY only supports BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY types');
    
    expect(function() {
      parquet_codec_delta_byte.decodeValues('BOOLEAN', { buffer: Buffer.alloc(10), offset: 0 }, 2, {});
    }).to.throw('DELTA_BYTE_ARRAY only supports BYTE_ARRAY and FIXED_LEN_BYTE_ARRAY types');
  });

  it('should throw error for invalid value types', function () {
    expect(function() {
      parquet_codec_delta_byte.encodeValues('BYTE_ARRAY', [123 as any], {});
    }).to.throw('Invalid value type for BYTE_ARRAY');
  });
});