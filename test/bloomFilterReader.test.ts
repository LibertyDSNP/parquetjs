import {expect} from "chai"
import { Int64 } from "thrift";
import { parseBloomFilterOffsets } from '../lib/bloomFilterIO/bloomFilterReader';
import {ColumnChunkData, ColumnChunkExt, ColumnMetaDataExt} from "../lib/types/types.js";

const emptyOffset = () => new Int64(Buffer.from(""), 0);

const emptyMetaData = (): ColumnMetaDataExt => {
  return {
    type: 0,
    encodings: [],
    path_in_schema: [],
    codec: 0,
    num_values: new Int64(0),
    total_compressed_size: new Int64(0),
    total_uncompressed_size: new Int64(0),
    key_value_metadata: [],
    data_page_offset: emptyOffset(),
    index_page_offset: emptyOffset(),
    dictionary_page_offset: emptyOffset(),
    statistics: {},
    encoding_stats: [],
    bloom_filter_offset: emptyOffset()
  }
}

describe("bloomFilterReader", () => {
  describe("offsets", () => {
    let columnChunkDataCollection: Array<ColumnChunkData>;


    beforeEach(() => {
      const metaData: ColumnMetaDataExt = emptyMetaData()
        metaData.path_in_schema = ["name"]
        metaData.bloom_filter_offset = new Int64(Buffer.from("000000000874", "hex"), 0)

      const columnData: ColumnChunkExt = {
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

