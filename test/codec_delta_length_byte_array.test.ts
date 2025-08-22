import { expect } from 'chai';
import * as parquet_codec_delta_length from '../lib/codec/delta_length_byte_array';

describe('ParquetCodec::DELTA_LENGTH_BYTE_ARRAY', function () {
  it('should encode and decode spec example: ["Hello", "World", "Foobar", "ABCDEF"]', function () {
    // Spec example: "Hello", "World", "Foobar", "ABCDEF"
    // Expected: DeltaEncoding(5, 5, 6, 6) + "HelloWorldFoobarABCDEF"
    const values = ["Hello", "World", "Foobar", "ABCDEF"];
    const opts = {};
    
    const encoded = parquet_codec_delta_length.encodeValues('BYTE_ARRAY', values, opts);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_delta_length.decodeValues('BYTE_ARRAY', cursor, values.length, opts);
    
    expect(decoded.map(buf => buf.toString())).to.deep.equal(values);
  });

  it('should encode and decode string values', function () {
    const values = ["apple", "banana", "cherry"];
    const opts = {};
    
    const encoded = parquet_codec_delta_length.encodeValues('BYTE_ARRAY', values, opts);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_delta_length.decodeValues('BYTE_ARRAY', cursor, values.length, opts);
    
    expect(decoded.map(buf => buf.toString())).to.deep.equal(values);
  });

  it('should encode and decode Buffer values', function () {
    const values = [
      Buffer.from([0x01, 0x02, 0x03]),
      Buffer.from([0x04, 0x05]),
      Buffer.from([0x06, 0x07, 0x08, 0x09])
    ];
    const opts = {};
    
    const encoded = parquet_codec_delta_length.encodeValues('BYTE_ARRAY', values, opts);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_delta_length.decodeValues('BYTE_ARRAY', cursor, values.length, opts);
    
    expect(decoded).to.deep.equal(values);
  });

  it('should encode and decode Uint8Array values', function () {
    const values = [
      new Uint8Array([0x01, 0x02]),
      new Uint8Array([0x03, 0x04, 0x05]),
      new Uint8Array([0x06])
    ];
    const opts = {};
    
    const encoded = parquet_codec_delta_length.encodeValues('BYTE_ARRAY', values, opts);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_delta_length.decodeValues('BYTE_ARRAY', cursor, values.length, opts);
    
    // Convert Uint8Arrays to Buffers for comparison
    const expectedBuffers = values.map(arr => Buffer.from(arr));
    expect(decoded).to.deep.equal(expectedBuffers);
  });

  it('should handle empty array', function () {
    const values: string[] = [];
    const opts = {};
    
    const encoded = parquet_codec_delta_length.encodeValues('BYTE_ARRAY', values, opts);
    expect(encoded.length).to.equal(0);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_delta_length.decodeValues('BYTE_ARRAY', cursor, values.length, opts);
    
    expect(decoded).to.deep.equal([]);
  });

  it('should handle empty strings', function () {
    const values = ["", "hello", "", "world"];
    const opts = {};
    
    const encoded = parquet_codec_delta_length.encodeValues('BYTE_ARRAY', values, opts);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_delta_length.decodeValues('BYTE_ARRAY', cursor, values.length, opts);
    
    expect(decoded.map(buf => buf.toString())).to.deep.equal(values);
  });

  it('should handle single value', function () {
    const values = ["single"];
    const opts = {};
    
    const encoded = parquet_codec_delta_length.encodeValues('BYTE_ARRAY', values, opts);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_delta_length.decodeValues('BYTE_ARRAY', cursor, values.length, opts);
    
    expect(decoded.map(buf => buf.toString())).to.deep.equal(values);
  });

  it('should throw error for unsupported types', function () {
    expect(function() {
      parquet_codec_delta_length.encodeValues('INT32', [1, 2, 3], {});
    }).to.throw('DELTA_LENGTH_BYTE_ARRAY only supports BYTE_ARRAY type');
    
    expect(function() {
      parquet_codec_delta_length.decodeValues('BOOLEAN', { buffer: Buffer.alloc(10), offset: 0 }, 2, {});
    }).to.throw('DELTA_LENGTH_BYTE_ARRAY only supports BYTE_ARRAY type');
  });

  it('should throw error for invalid value types', function () {
    expect(function() {
      parquet_codec_delta_length.encodeValues('BYTE_ARRAY', [123 as any], {});
    }).to.throw('Invalid value type for BYTE_ARRAY');
  });
});