const parquet_util = require("./util");
const parquet_thrift = require("../gen-nodejs/parquet_types");
const sbbf = require("./bloom/sbbf").default;
import {ColumnChunk} from "./types/types";

import { ParquetEnvelopeReader } from "parquet";

const filterColumnChunksWithBloomFilters = (columnChunks: Array<ColumnChunk>) => {
  return columnChunks.filter((columnChunk) => {
    const {
      column: {
        meta_data: {
          bloom_filter_offset: { buffer: bloomFilterOffsetBuffer },
        },
      },
    } = columnChunk;
    return bloomFilterOffsetBuffer;
  });
};


const toInteger = (buffer: Buffer) => {
  const hexPrefix = "0x";
  return parseInt(hexPrefix + buffer.toString("hex"));
};

type bloomFilterOffsetData = {
  columnName: string,
  offsetBytes: number,
  rowGroup: number
}

const parseBloomFilterOffsets = (columnChunks: Array<ColumnChunk>): Array<bloomFilterOffsetData> => {
  return columnChunks.map((columnChunk) => {
    const {
      column: {
        meta_data: {
          bloom_filter_offset: { buffer: bloomFilterOffsetBuffer },
          path_in_schema: pathInSchema,
        },
      },
      rowGroup,
    } = columnChunk;

    return {
      offsetBytes: toInteger(bloomFilterOffsetBuffer),
      columnName: pathInSchema.join(""),
      rowGroup: rowGroup
    };
  });
};

const getBloomFilterHeader = async (offsetBytes: number, envelopeReader: ParquetEnvelopeReader) => {
  const headerByteSizeEstimate = 200;
  const bloomFilterHeaderData = await envelopeReader.read(
    offsetBytes,
    headerByteSizeEstimate
  );
  const bloomFilterHeader = new parquet_thrift.BloomFilterHeader();
  const sizeOfBloomFilterHeader = parquet_util.decodeThrift(
    bloomFilterHeader,
    bloomFilterHeaderData
  );

  return {
    bloomFilterHeader,
    sizeOfBloomFilterHeader,
  };
};

const readFilterData = async (offsetBytes: number,
                              envelopeReader: ParquetEnvelopeReader): Promise<Buffer> => {

  const {
    bloomFilterHeader,
    sizeOfBloomFilterHeader,
  } = await getBloomFilterHeader(offsetBytes, envelopeReader);

  const { numBytes: filterByteSize } = bloomFilterHeader;

  try {
    const filterBlocksOffset = offsetBytes + sizeOfBloomFilterHeader;
    const buffer = await envelopeReader.read(
      filterBlocksOffset,
      filterByteSize
    );

    return buffer;
  } catch (e) {
    throw new Error(e)
  }
};

const readFilterDataFrom = (offsets: Array<number>, envelopeReader: ParquetEnvelopeReader): Promise<Array<Buffer>> => {
  return Promise.all(
    offsets.map(async (offset) => await readFilterData(offset, envelopeReader))
  );
};

const siftAllOffsets = (columnChunks: Array<ColumnChunk>): Array<bloomFilterOffsetData> => {
  return parseBloomFilterOffsets(filterColumnChunksWithBloomFilters(columnChunks));
};

const readFilterBlocksFrom = async (offsets: Array<bloomFilterOffsetData>, envelopeReader: ParquetEnvelopeReader) => {
  const offsetByteValues = offsets.map(({ offsetBytes }) => offsetBytes);

  const filterBlocksBuffers: Array<Buffer> = await readFilterDataFrom(
    offsetByteValues,
    envelopeReader
  );

  return filterBlocksBuffers.reduce((accumulator: Record<string, any>, buffer: Buffer, index: number) => {
    const { columnName, rowGroup } = offsets[index];

    if (!(columnName in accumulator)) {
      accumulator[columnName] = [];
    }

    accumulator[columnName].push({
      sbbf: sbbf.from(buffer),
      columnName,
      rowGroup,
    });
    return accumulator;
  }, {});
};

module.exports = {
  siftAllOffsets,
  readFilterBlocksFrom,
  parseBloomFilterOffsets,
};
