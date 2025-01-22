import {expect} from 'chai';
import {decodeRunBitpacked} from '../../../lib/codec/rle';
import {readRleBitPackedHybrid} from "../../../lib/codec/encoding";

describe('RLE Codec', function () {
  it('can decode a known bitpack value', function () {
    // 136  = 10001000,  0x80 = 128 or 100000000
    const bitPackedDecBuffer = Buffer.from([136, 1, 7, 251, 127, 127, 28, 1, 9, 254, 251, 191, 63]);

    const myView = new DataView(bitPackedDecBuffer.buffer);
    let decoder = new TextDecoder('utf-8');
    const res = bitPackedDecBuffer.map((val, i, ary) => myView.getUint8(i));
    // Answer was `var N1 = Mat` which is part of the compiled parquetjs library.
    // Undid some changes and now it's `module.exports = read`...
    console.log(decoder.decode(res));

    const reader = {view: myView, offset: 0};
    const values = new Array(26).fill(0);
    const expected = [
      1, 1, 1, 1, 0, 1, 1, 0,
      0, 1, 1, 1, 0, 1, 1, 0,
      1, 1, 0, 0, 1, 1, 1, 0,
      0, 0
    ];
    readRleBitPackedHybrid(reader, 1, 13, values, true);
    expect(values.length).equals(expected.length)
    // correct?
    values.forEach((val, i) => {
      expect(val, `${val} != ${expected[i]} for i = ${i}`).equals(expected[i])
    })
  });

  describe('#decodeRunBitpacked', function () {
    // use the example from the documentation for RLE/Bitpacked hybrid,
    // https://parquet.apache.org/docs/file-format/data-pages/encodings/#RLE
    it('writes and reads bit packed values for documentation example correctly', () => {
      const decVals = [0, 1, 2, 3, 4, 5, 6, 7]

      const bytesOfEncodedData = 4
      const buffer = new ArrayBuffer(bytesOfEncodedData)

      const view = new DataView(buffer);

      // the number of values? or the number of bits used for encoding a set of values ?
      const bitPackedRunLength = 8;
      const bitPackedScaledRunLength = bitPackedRunLength / 8;
      // in the grammar it says it's EITHER the left-shifted value OR 1 if the shifted value is 0?? but that makes
      // no sense and results in an error.
      const shiftedBPSRL = (bitPackedScaledRunLength << 1) | 1;

      view.setUint8(0, shiftedBPSRL);

      // byte values 1-3, from the example
      view.setUint8(1, 0b10001000);
      view.setUint8(2, 0b11000110);
      view.setUint8(3, 0b11111010);

      const bitWidth = 3;

      // number of expected values in the result = 8
      const cursor = {buffer: Buffer.from(view.buffer), offset: 0}
      const values = decodeRunBitpacked(cursor, decVals.length, {bitWidth});
      expect(values.length).equals(decVals.length);
      values.forEach((val, i) => {
        expect(val).equals(decVals[i]);
      })
    });
  });
});
