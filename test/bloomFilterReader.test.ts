import {expect} from "chai"
const bloomFilterReader = require("../lib/bloomFilterReader.js");

describe("bloomFilterReader", () => {
  describe("offsets", () => {
    let columnChunkMeta = {};

    beforeEach(() => {
      const metaData = {
        encodings: [3, 0],
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
          rowGroup: 0,
          column: {
            file_path: null,
            meta_data: metaData,
            encrypted_column_metadata: null,
          },
        },
      ];
    });

    it("returns bloom filter offsets", () => {
      const result = bloomFilterReader.parseBloomFilterOffsets(columnChunkMeta);
      const expected = [
        {
          columnName: "name",
          offset: 2164,
          rowGroup: 0,
        },
      ];

      expect(result).to.deep.equal(expected);
    });
  })
});

