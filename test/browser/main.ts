import * as parquetjs from "../../dist/browser/parquet.esm";
import { assert } from "chai";

const buffer = require("buffer");

describe("Browser tests", () => {
  describe("reader", () => {
    it("can read snappy compressed data", async () => {
      // Data from test/test-files/snappy-compressed.parquet
      const uint8Array = [80, 65, 82, 49, 21, 6, 21, 80, 21, 82, 92, 21, 8, 21, 0, 21, 8, 21, 0, 21, 0, 21, 0, 17, 28, 24, 5, 119, 111, 114, 108, 100, 24, 8, 49, 112, 111, 97, 52, 98, 112, 102, 22, 0, 22, 8, 24, 5, 119, 111, 114, 108, 100, 24, 8, 49, 112, 111, 97, 52, 98, 112, 102, 0, 0, 0, 40, 32, 5, 0, 0, 0, 104, 101, 108, 108, 111, 1, 9, 104, 119, 111, 114, 108, 100, 6, 0, 0, 0, 98, 97, 110, 97, 110, 97, 8, 0, 0, 0, 49, 112, 111, 97, 52, 98, 112, 102, 21, 12, 25, 37, 6, 0, 25, 24, 16, 99, 111, 109, 112, 114, 101, 115, 115, 101, 100, 83, 116, 114, 105, 110, 103, 21, 2, 22, 8, 22, 206, 1, 22, 206, 1, 38, 8, 60, 24, 5, 119, 111, 114, 108, 100, 24, 8, 49, 112, 111, 97, 52, 98, 112, 102, 22, 0, 22, 8, 24, 5, 119, 111, 114, 108, 100, 24, 8, 49, 112, 111, 97, 52, 98, 112, 102, 0, 0, 41, 24, 8, 49, 112, 111, 97, 52, 98, 112, 102, 25, 24, 5, 119, 111, 114, 108, 100, 0, 25, 28, 22, 8, 21, 206, 1, 22, 0, 0, 0, 21, 2, 25, 44, 72, 4, 114, 111, 111, 116, 21, 2, 0, 21, 12, 37, 0, 24, 16, 99, 111, 109, 112, 114, 101, 115, 115, 101, 100, 83, 116, 114, 105, 110, 103, 37, 0, 0, 22, 8, 25, 28, 25, 28, 38, 214, 1, 28, 21, 12, 25, 37, 6, 0, 25, 24, 16, 99, 111, 109, 112, 114, 101, 115, 115, 101, 100, 83, 116, 114, 105, 110, 103, 21, 2, 22, 8, 22, 206, 1, 22, 206, 1, 38, 8, 60, 24, 5, 119, 111, 114, 108, 100, 24, 8, 49, 112, 111, 97, 52, 98, 112, 102, 22, 0, 22, 8, 24, 5, 119, 111, 114, 108, 100, 24, 8, 49, 112, 111, 97, 52, 98, 112, 102, 0, 0, 22, 154, 3, 21, 22, 22, 242, 2, 21, 40, 0, 22, 234, 2, 22, 8, 0, 25, 12, 24, 15, 64, 100, 115, 110, 112, 47, 112, 97, 114, 113, 117, 101, 116, 106, 115, 0, 163, 0, 0, 0, 80, 65, 82, 49];
      const snappyCompressedBuffer = buffer.Buffer.from(uint8Array);
      const reader = await parquetjs.ParquetReader.openBuffer(snappyCompressedBuffer);
      const data: any[] = [];
      for await (const record of reader) {
        data.push(record);
      }
      assert.equal(data.length, 4);

      after(async () => {
        await reader.close();
      })
    });
  });
});