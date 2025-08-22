import { expect } from 'chai';
import * as parquet_codec_delta from '../lib/codec/delta_binary_packed';

describe('ParquetCodec::DELTA_BINARY_PACKED', function () {
  it('should encode and decode Example 1 from spec: [1,2,3,4,5]', function () {
    // Example 1 from spec: 1, 2, 3, 4, 5
    // Expected: deltas [1,1,1,1], min_delta=1, relative_deltas=[0,0,0,0]
    const values = [1, 2, 3, 4, 5];
    const opts = {};
    
    const encoded = parquet_codec_delta.encodeValues('INT32', values, opts);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_delta.decodeValues('INT32', cursor, values.length, opts);
    
    expect(decoded).to.deep.equal(values);
  });

  it('should encode and decode Example 2 from spec: [7,5,3,1,2,3,4,5]', function () {
    // Example 2 from spec: 7, 5, 3, 1, 2, 3, 4, 5
    // Expected: deltas [-2,-2,-2,1,1,1,1], min_delta=-2, relative_deltas=[0,0,0,3,3,3,3]
    const values = [7, 5, 3, 1, 2, 3, 4, 5];
    const opts = {};
    
    const encoded = parquet_codec_delta.encodeValues('INT32', values, opts);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_delta.decodeValues('INT32', cursor, values.length, opts);
    
    expect(decoded).to.deep.equal(values);
  });

  it('should encode and decode INT32 values', function () {
    const values = [42, 17, 23, -1, -2, -3, 9000, 420];
    const opts = {};
    
    const encoded = parquet_codec_delta.encodeValues('INT32', values, opts);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_delta.decodeValues('INT32', cursor, values.length, opts);
    
    expect(decoded).to.deep.equal(values);
  });

  it('should encode and decode INT64 values', function () {
    const values = [42, 17, 23, -1, -2, -3, 9000, 420];
    const opts = {};
    
    const encoded = parquet_codec_delta.encodeValues('INT64', values, opts);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_delta.decodeValues('INT64', cursor, values.length, opts);
    
    expect(decoded).to.deep.equal(values);
  });

  it('should handle empty array', function () {
    const values = [];
    const opts = {};
    
    const encoded = parquet_codec_delta.encodeValues('INT32', values, opts);
    expect(encoded.length).to.equal(0);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_delta.decodeValues('INT32', cursor, values.length, opts);
    
    expect(decoded).to.deep.equal(values);
  });

  it('should handle single value', function () {
    const values = [42];
    const opts = {};
    
    const encoded = parquet_codec_delta.encodeValues('INT32', values, opts);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_delta.decodeValues('INT32', cursor, values.length, opts);
    
    expect(decoded).to.deep.equal(values);
  });

  it('should handle large values with overflow', function () {
    // Test values that may cause arithmetic overflow
    const values = [2147483647, -2147483648, 2147483646, -2147483647];
    const opts = {};
    
    const encoded = parquet_codec_delta.encodeValues('INT32', values, opts);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_delta.decodeValues('INT32', cursor, values.length, opts);
    
    expect(decoded).to.deep.equal(values);
  });

  it('should handle repeated values', function () {
    const values = [1, 1, 1, 1, 1];
    const opts = {};
    
    const encoded = parquet_codec_delta.encodeValues('INT32', values, opts);
    
    const cursor = { buffer: encoded, offset: 0 };
    const decoded = parquet_codec_delta.decodeValues('INT32', cursor, values.length, opts);
    
    expect(decoded).to.deep.equal(values);
  });

  it('should throw error for unsupported types', function () {
    expect(function() {
      parquet_codec_delta.encodeValues('BOOLEAN', [true, false], {});
    }).to.throw('DELTA_BINARY_PACKED only supports INT32 and INT64 types');
    
    expect(function() {
      parquet_codec_delta.decodeValues('FLOAT', { buffer: Buffer.alloc(10), offset: 0 }, 2, {});
    }).to.throw('DELTA_BINARY_PACKED only supports INT32 and INT64 types');
  });
});