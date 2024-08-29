import { expect } from 'chai';
import { decodeRunBitpacked } from '../../../lib/codec/rle';
import type { Cursor } from '../../../lib/codec/types';

describe('RLE Codec', function () {
  describe('#decodeRunBitpacked', function () {
    it('can decode a known bitpack value', function () {
      const cursor: Cursor = {
        // 136, 1, left off the front
        buffer: Buffer.from([7, 251, 127, 127, 28, 1, 9, 254, 251, 191, 63]),
        offset: 0,
      };
      const values = decodeRunBitpacked(cursor, 24, { bitWidth: 1 });
      expect(values.length).equals(24);
      console.log(values)
    });

    // use the example from the documentation for RLE/Bitpacked hybrid,
    // https://parquet.apache.org/docs/file-format/data-pages/encodings/#RLE
    it('writes and reads bit packed values for documentation example correctly', () => {
      const decVals = [0, 1, 2, 3, 4, 5, 6, 7]

      const bytesOfEncodedData = 4
      const buffer = new ArrayBuffer(bytesOfEncodedData)

      const view = new DataView(buffer);

      // the number of values? or the number of bits used for encoding a set of values ?
      const bitPackedRunLength = 8;
      const bitPackedScaledRunLength = bitPackedRunLength/8;
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
      const cursor = { buffer: Buffer.from(view.buffer), offset: 0 }
      const values = decodeRunBitpacked(cursor, decVals.length, {bitWidth} );
      expect(values.length).equals(decVals.length);
      values.forEach((val,i) => {
        expect(val).equals(decVals[i]);
      })
    });
  });
});
