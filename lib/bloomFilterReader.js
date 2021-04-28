const parquet_util = require("./util");
const parquet_thrift = require("../gen-nodejs/parquet_types");
const sbbf = require("./bloom/sbbf").default;

const filterColumnChunksWithBloomFilters = (columnChunks) => {
  return columnChunks.filter((columnData) => {
    const {
      column: {
        meta_data: {
          bloom_filter_offset: { buffer: bloomFilterOffsetBuffer },
        },
      },
    } = columnData;

    return bloomFilterOffsetBuffer;
  });
};

const toInteger = (buffer) => {
  const hexPrefix = "0x";
  return parseInt(hexPrefix + buffer.toString("hex"));
};

const parseBloomFilterOffsets = (columnChunksMeta) => {
  return columnChunksMeta.map((columnChunk) => {
    const {
      column: {
        meta_data: {
          bloom_filter_offset: { buffer: bloomFilterOffsetBuffer },
          path_in_schema: pathInSchema,
        },
      },
      rowGroup,
    } = columnChunk;

    const offset = toInteger(bloomFilterOffsetBuffer);

    const columnName = pathInSchema.join("");

    return {
      columnName,
      offset,
      rowGroup,
    };
  });
};

const getBloomFilterHeader = async (bloomFilterOffset, envelopeReader) => {
  const headerByteSizeEstimate = 200;
  const bloomFilterHeaderData = await envelopeReader.read(
    bloomFilterOffset,
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

const readFilterData = async (offset, envelopeReader) => {
  const {
    bloomFilterHeader,
    sizeOfBloomFilterHeader,
  } = await getBloomFilterHeader(offset, envelopeReader);

  const { numBytes: filterByteSize } = bloomFilterHeader;

  try {
    const filterBlocksOffset = offset + sizeOfBloomFilterHeader;
    const buffer = await envelopeReader.read(
      filterBlocksOffset,
      filterByteSize
    );

    return buffer;
  } catch (e) {
    console.log("porque parquet? error: ", e);
  }
};

const readFilterDataFrom = async (offsets, envelopeReader) => {
  return await Promise.all(
    offsets.map(async (offset) => await readFilterData(offset, envelopeReader))
  );
};

const siftAllOffsets = (columnChunks) => {
  const chunks = filterColumnChunksWithBloomFilters(columnChunks);
  const offsets = parseBloomFilterOffsets(chunks);

  return offsets;
};

const readFilterBlocksFrom = async (offsets, envelopeReader) => {
  const filterOffsets = offsets.map(({ offset }) => offset);

  const filterBlocksBuffers = await readFilterDataFrom(
    filterOffsets,
    envelopeReader
  );

  return filterBlocksBuffers.reduce((accumulator, buffer, index) => {
    const { columnName, rowGroup } = offsets[index];

    if (columnName in accumulator === false) {
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
