'use strict';

const chai = require('chai');
const os = require('os');
const path = require('path');
const fs = require('fs');
const assert = chai.assert;
const parquet = require('../parquet');

// Regression test for ERR_OUT_OF_RANGE crash in encodeValues_BYTE_ARRAY.
// Each emoji is 4 UTF-8 bytes but only 2 JS chars (surrogate pair), so
// STR_PAD=8 underestimates byte length. After 10 such strings buf_pos drifts
// past buf_len, and the 11th buf.write() throws:
//   RangeError [ERR_OUT_OF_RANGE]: The value of "offset" is out of range.
describe('emoji string encoding', function () {
  const tmpFile = path.join(os.tmpdir(), 'emoji_test.parquet');

  after(function () {
    if (fs.existsSync(tmpFile)) fs.unlinkSync(tmpFile);
  });

  it('should write and read a parquet file containing many emoji UTF8 strings without throwing', async function () {
    const schema = new parquet.ParquetSchema({ name: { type: 'UTF8' } });
    const writer = await parquet.ParquetWriter.openFile(schema, tmpFile);

    // 11 rows of 5-emoji strings is the minimum to trigger the crash:
    // each string drifts buf_pos by +2, after 10 strings buf_pos+4 > buf_len.
    for (let i = 0; i < 11; i++) {
      await writer.appendRow({ name: '🎉🎉🎉🎉🎉' });
    }
    await writer.close();

    const reader = await parquet.ParquetReader.openFile(tmpFile);
    const cursor = reader.getCursor();
    const rows = [];
    let row;
    while ((row = await cursor.next())) rows.push(row);
    await reader.close();

    assert.equal(rows.length, 11);
    assert.isTrue(rows.every((r) => r.name === '🎉🎉🎉🎉🎉'));
  });
});
