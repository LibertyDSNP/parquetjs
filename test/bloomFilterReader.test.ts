import {expect} from "chai"
import { parseBloomFilterOffsets } from '../lib/bloomFilterReader';
import { ColumnChunk, ColumnChunkData, ColumnData } from "../lib/types/types.js";
// const bloomFilterReader = require("../lib/bloomFilterReader.js");

describe("bloomFilterReader", () => {
  describe("offsets", () => {
    let columnChunkMeta: Array<any>;


    beforeEach(() => {
      const metaData = {
        path_in_schema: ["name"],
        codec: 0,
        statistics: {},
        encoding_stats: null,
        bloom_filter_offset: {
          buffer: Buffer.from("000000000874", "hex"),
          offset: 0,
        },
      };

      columnChunkMeta = [
        {
          rowGroupIndex: 0,
          column: {
            meta_data: metaData,
            encrypted_column_metadata: null,
          },
        },
      ];
    });

    it("returns bloom filter offsets", () => {
      const result = parseBloomFilterOffsets(columnChunkMeta);
      const expected = [
        {
          columnName: "name",
          offsetBytes: 2164,
          rowGroupIndex: 0,
        },
      ];

      expect(result).to.deep.equal(expected);
    });
  })
});

