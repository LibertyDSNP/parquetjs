import { expect } from 'chai';
import * as parquet_codec_stream_split from '../lib/codec/byte_stream_split';

describe('ParquetCodec::BYTE_STREAM_SPLIT', function () {
  it('should encode and decode spec example: three 32-bit floats', function () {
    // Spec example: Element 0: AA BB CC DD, Element 1: 00 11 22 33, Element 2: A3 B4 C5 D6
    // Expected output: AA 00 A3 BB 11 B4 CC 22 C5 DD 33 D6
    const element0 = Buffer.from([0xAA, 0xBB, 0xCC, 0xDD]);
    const element1 = Buffer.from([0x00, 0x11, 0x22, 0x33]);  
    const element2 = Buffer.from([0xA3, 0xB4, 0xC5, 0xD6]);
    const values = [element0, element1, element2];
    const opts = { typeLength: 4 };
    
    const encoded = parquet_codec_stream_split.encodeValues('FIXED_LEN_BYTE_ARRAY', values, opts);
    
    // Verify the exact byte pattern from spec
    const expected = Buffer.from([0xAA, 0x00, 0xA3, 0xBB, 0x11, 0xB4, 0xCC, 0x22, 0xC5, 0xDD, 0x33, 0xD6]);
    expect(encoded).to.deep.equal(expected);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_stream_split.decodeValues('FIXED_LEN_BYTE_ARRAY', cursor, values.length, opts);
    
    expect(decoded).to.deep.equal(values);
  });

  it('should encode and decode FLOAT values', function () {
    const values = [1.5, 2.5, 3.5, 4.5];
    const opts = {};
    
    const encoded = parquet_codec_stream_split.encodeValues('FLOAT', values, opts);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_stream_split.decodeValues('FLOAT', cursor, values.length, opts);
    
    expect(decoded).to.deep.equal(values);
  });

  it('should encode and decode DOUBLE values', function () {
    const values = [1.123456789, 2.987654321, 3.141592653];
    const opts = {};
    
    const encoded = parquet_codec_stream_split.encodeValues('DOUBLE', values, opts);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_stream_split.decodeValues('DOUBLE', cursor, values.length, opts);
    
    // Use approximate equality for floating point comparison
    expect(decoded).to.have.lengthOf(values.length);
    for (let i = 0; i < values.length; i++) {
      expect(decoded[i]).to.be.closeTo(values[i] as number, 1e-10);
    }
  });

  it('should encode and decode INT32 values', function () {
    const values = [0x12345678, 0x9ABCDEF0, 0x11111111, 0x22222222];
    const opts = {};
    
    const encoded = parquet_codec_stream_split.encodeValues('INT32', values, opts);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_stream_split.decodeValues('INT32', cursor, values.length, opts);
    
    expect(decoded).to.deep.equal(values);
  });

  it('should encode and decode INT64 values', function () {
    const values = [0x123456789ABCDEF0, 0xFEDCBA9876543210, 0x1111111111111111];
    const opts = {};
    
    const encoded = parquet_codec_stream_split.encodeValues('INT64', values, opts);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_stream_split.decodeValues('INT64', cursor, values.length, opts);
    
    expect(decoded).to.deep.equal(values);
  });

  it('should encode and decode FIXED_LEN_BYTE_ARRAY values', function () {
    const values = [
      Buffer.from([0x01, 0x02, 0x03]),
      Buffer.from([0x04, 0x05, 0x06]),
      Buffer.from([0x07, 0x08, 0x09])
    ];
    const opts = { typeLength: 3 };
    
    const encoded = parquet_codec_stream_split.encodeValues('FIXED_LEN_BYTE_ARRAY', values, opts);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_stream_split.decodeValues('FIXED_LEN_BYTE_ARRAY', cursor, values.length, opts);
    
    expect(decoded).to.deep.equal(values);
  });

  it('should encode and decode Uint8Array values', function () {
    const values = [
      new Uint8Array([0xAA, 0xBB]),
      new Uint8Array([0xCC, 0xDD]),
      new Uint8Array([0xEE, 0xFF])
    ];
    const opts = { typeLength: 2 };
    
    const encoded = parquet_codec_stream_split.encodeValues('FIXED_LEN_BYTE_ARRAY', values, opts);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_stream_split.decodeValues('FIXED_LEN_BYTE_ARRAY', cursor, values.length, opts);
    
    // Convert expected Uint8Arrays to Buffers
    const expectedBuffers = values.map(arr => Buffer.from(arr));
    expect(decoded).to.deep.equal(expectedBuffers);
  });

  it('should handle empty array', function () {
    const values: number[] = [];
    const opts = {};
    
    const encoded = parquet_codec_stream_split.encodeValues('FLOAT', values, opts);
    expect(encoded.length).to.equal(0);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_stream_split.decodeValues('FLOAT', cursor, values.length, opts);
    
    expect(decoded).to.deep.equal([]);
  });

  it('should handle single value', function () {
    const values = [42.5];
    const opts = {};
    
    const encoded = parquet_codec_stream_split.encodeValues('FLOAT', values, opts);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_stream_split.decodeValues('FLOAT', cursor, values.length, opts);
    
    expect(decoded).to.deep.equal(values);
  });

  it('should throw error for unsupported types', function () {
    expect(function() {
      parquet_codec_stream_split.encodeValues('BOOLEAN', [true, false], {});
    }).to.throw('BYTE_STREAM_SPLIT only supports FLOAT, DOUBLE, INT32, INT64, FIXED_LEN_BYTE_ARRAY types');
    
    expect(function() {
      parquet_codec_stream_split.decodeValues('BYTE_ARRAY', { buffer: Buffer.alloc(10), offset: 0 }, 2, {});
    }).to.throw('BYTE_STREAM_SPLIT only supports FLOAT, DOUBLE, INT32, INT64, FIXED_LEN_BYTE_ARRAY types');
  });

  it('should throw error when typeLength missing for FIXED_LEN_BYTE_ARRAY', function () {
    expect(function() {
      parquet_codec_stream_split.encodeValues('FIXED_LEN_BYTE_ARRAY', [Buffer.from([1,2])], {});
    }).to.throw('typeLength is required for FIXED_LEN_BYTE_ARRAY');
    
    expect(function() {
      parquet_codec_stream_split.decodeValues('FIXED_LEN_BYTE_ARRAY', { buffer: Buffer.alloc(10), offset: 0 }, 2, {});
    }).to.throw('typeLength is required for FIXED_LEN_BYTE_ARRAY');
  });

  it('should throw error for mismatched value sizes', function () {
    expect(function() {
      parquet_codec_stream_split.encodeValues('FIXED_LEN_BYTE_ARRAY', [Buffer.from([1,2]), Buffer.from([3])], { typeLength: 2 });
    }).to.throw('Value size 1 does not match expected size 2 for type FIXED_LEN_BYTE_ARRAY');
  });

  it('should throw error for invalid value types', function () {
    expect(function() {
      parquet_codec_stream_split.encodeValues('FLOAT', [null as any], {});
    }).to.throw('Invalid value type for FLOAT');
  });
});