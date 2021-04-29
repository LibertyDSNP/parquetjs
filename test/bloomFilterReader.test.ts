import {expect} from "chai"
import { parseBloomFilterOffsets } from '../lib/bloomFilterReader';
import {ColumnChunkData, ColumnData, ColumnMetaData, Offset} from "../lib/types/types.js";

const emptyOffset= ():Offset => {
  return { buffer: Buffer.from(""), offset: 0 }
}
const emptyMetaData = (): ColumnMetaData => {
  return {
    type: 0,
    encodings: [],
    path_in_schema: [],
    codec: 0,
    num_values: 0,
    total_compressed_size: 0,
    total_uncompressed_size: 0,
    key_value_metadata: {},
    data_page_offset: emptyOffset(),
    index_page_offset: emptyOffset(),
    dictionary_page_offset: emptyOffset(),
    statistics: {},
    encoding_stats: {},
    bloom_filter_offset: emptyOffset()
  }
}

describe("bloomFilterReader", () => {
  describe("offsets", () => {
    let columnChunkDataCollection: Array<ColumnChunkData>;


    beforeEach(() => {
      const metaData: ColumnMetaData = emptyMetaData()
        metaData.path_in_schema = ["name"]
        metaData.bloom_filter_offset = {
          buffer: Buffer.from("000000000874", "hex"),
          offset: 0,
        }

      const columnData: ColumnData = {
        meta_data: metaData,
        file_offset: emptyOffset(),
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
      const result = parseBloomFilterOffsets(columnChunkDataCollection);
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

