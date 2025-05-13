'use strict';
const chai = require('chai');
const assert = chai.assert;
const parquet_codec_rle = require('../lib/codec/rle');
const {readRleBitPackedHybrid} = require("../lib/codec/encoding");

function dataViewFromArray(data) {
  const ab =  new ArrayBuffer(data.length,{ maxByteLength: data.length });
  let view = new DataView(ab, 0);
  data.forEach((val,idx) => view.setUint8(idx, val));
  return view
}

describe('ParquetCodec::RLE', function () {
  it('should encode bitpacked values', function () {
    let buf = parquet_codec_rle.encodeValues('INT32', [0, 1, 2, 3, 4, 5, 6, 7], {
      disableEnvelope: true,
      bitWidth: 3,
    });

    assert.deepEqual(buf, Buffer.from([0x03, 0x88, 0xc6, 0xfa]));
  });

  it('should decode bitpacked values', function () {
    let vals = parquet_codec_rle.decodeValues(
      'INT32',
      {
        buffer: Buffer.from([0x03, 0x88, 0xc6, 0xfa]),
        offset: 0,
      },
      8,
      {
        disableEnvelope: true,
        bitWidth: 3,
      }
    );

    assert.deepEqual(vals, [0, 1, 2, 3, 4, 5, 6, 7]);
  });

  describe('number of values not a multiple of 8', function () {
    it('should encode bitpacked values', function () {
      let buf = parquet_codec_rle.encodeValues('INT32', [0, 1, 2, 3, 4, 5, 6, 7, 6, 5], {
        disableEnvelope: true,
        bitWidth: 3,
      });

      assert.deepEqual(buf, Buffer.from([0x05, 0x88, 0xc6, 0xfa, 0x2e, 0x00, 0x00]));
    });

    it('should decode bitpacked values', function () {
      let vals = parquet_codec_rle.decodeValues(
        'INT32',
        {
          buffer: Buffer.from([0x05, 0x88, 0xc6, 0xfa, 0x2e, 0x00, 0x00]),
          offset: 0,
        },
        10,
        {
          disableEnvelope: true,
          bitWidth: 3,
        }
      );

      assert.deepEqual(vals, [0, 1, 2, 3, 4, 5, 6, 7, 6, 5]);
    });
  });

  it('should encode repeated values', function () {
    let buf = parquet_codec_rle.encodeValues(
      'INT32',
      [1234567, 1234567, 1234567, 1234567, 1234567, 1234567, 1234567, 1234567],
      {
        disableEnvelope: true,
        bitWidth: 21,
      }
    );

    assert.deepEqual(buf, Buffer.from([0x10, 0x87, 0xd6, 0x12]));
  });

  it('should decode repeated values', function () {
    const data = [0x10, 0x87, 0xd6, 0x12];
    let cursor = {
      buffer: Buffer.from(data),
      offset: 0,
    };
    const expectedDecoded = [1234567, 1234567, 1234567, 1234567, 1234567, 1234567, 1234567, 1234567]
    const bitWidth = 21;
    const disableEnvelope = true;
    let vals = parquet_codec_rle.decodeValues(
      'UNUSED',
      cursor,
      expectedDecoded.length,
      { disableEnvelope, bitWidth}
    );

    assert.deepEqual(vals, expectedDecoded);

    let view = dataViewFromArray(data);
    const reader =  { view , offset: 0 };
    let output = new Array(expectedDecoded.length);
    readRleBitPackedHybrid(reader, bitWidth, data.length, output, disableEnvelope)

    assert.deepEqual(output, expectedDecoded);

  });

  it('should encode mixed runs', function () {
    let buf = parquet_codec_rle.encodeValues(
      'INT32',
      [0, 1, 2, 3, 4, 5, 6, 7, 4, 4, 4, 4, 4, 4, 4, 4, 0, 1, 2, 3, 4, 5, 6, 7],
      {
        disableEnvelope: true,
        bitWidth: 3,
      }
    );

    assert.deepEqual(buf, Buffer.from([0x03, 0x88, 0xc6, 0xfa, 0x10, 0x04, 0x03, 0x88, 0xc6, 0xfa]));
  });

  it('should decode mixed runs', function () {
    const expectedDecoded = [0, 1, 2, 3, 4, 5, 6, 7, 4, 4, 4, 4, 4, 4, 4, 4, 0, 1, 2, 3, 4, 5, 6, 7];
    const data = [0x03, 0x88, 0xc6, 0xfa, 0x10, 0x04, 0x03, 0x88, 0xc6, 0xfa];
    const disableEnvelope = true;
    const bitWidth = 3;

    let vals = parquet_codec_rle.decodeValues(
      'UNUSED',
      {
        buffer: Buffer.from(data),
        offset: 0,
      },
      expectedDecoded.length,
      { disableEnvelope, bitWidth });

    assert.deepEqual(vals,expectedDecoded);

    let view = dataViewFromArray(data);
    const reader =  { view , offset: 0 };
    let output = new Array(expectedDecoded.length);
    readRleBitPackedHybrid(reader, bitWidth, data.length, output, disableEnvelope)

    assert.deepEqual(output, expectedDecoded);

  });
});
