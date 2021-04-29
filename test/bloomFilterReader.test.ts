import {expect} from "chai"
import { parseBloomFilterOffsets } from '../lib/bloomFilterReader';
import { ColumnChunkData, ColumnData } from "../lib/types/types.js";
// const bloomFilterReader = require("../lib/bloomFilterReader.js");

describe("bloomFilterReader", () => {
  describe("offsets", () => {
    let columnChunkDataCollection: Array<Partial<ColumnChunkData>>;


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

      const columnData: Partial<ColumnData> = {
        meta_data: metaData,
        file_offset: {buffer: null, offset: 0},
        file_path: ''
      }

      columnChunkDataCollection = [
        {
          rowGroupIndex: 0,
          column: columnData,
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

