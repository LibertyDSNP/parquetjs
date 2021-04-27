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
  return columnChunksMeta.reduce((accumulator, columnChunk) => {
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

    if (columnName in accumulator === false) {
      accumulator[columnName] = [];
    }

    accumulator[columnName].push({
      offset,
      rowGroup,
    });

    return accumulator;
  }, {});
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

const readFilterData = async (bloomFilterOffset, envelopeReader) => {
  const {
    bloomFilterHeader,
    sizeOfBloomFilterHeader,
  } = await getBloomFilterHeader(bloomFilterOffset, envelopeReader);

  const { numBytes: filterByteSize } = bloomFilterHeader;

  try {
    const offset = bloomFilterOffset + sizeOfBloomFilterHeader;
    const buffer = await envelopeReader.read(offset, filterByteSize);

    return buffer;
  } catch (e) {
    console.log("porque parquet? error: ", e);
  }
};

const readFilterOffsets = async (bloomOffsets, envelopeReader) => {
  return await Promise.all(
    bloomOffsets.map(async ({ offset, rowGroup }) => {
      const filterBuffer = await readFilterData(offset, envelopeReader);

      const filterBlocks = sbbf.from(filterBuffer);

      return {
        rowGroup,
        filterBlocks,
      };
    })
  );
};

const siftAllOffsetByColumn = (columnChunks) => {
  const chunks = filterColumnChunksWithBloomFilters(columnChunks);
  const offsets = parseBloomFilterOffsets(chunks);

  return offsets;
};

const readFilterBlocksFrom = async (offsets, envelopeReader) => {
  const columnNames = Object.keys(offsets);

  const promises = columnNames.map(async (columnName) => {
    const columnOffsets = offsets[columnName];
    const filters = await readFilterOffsets(columnOffsets, envelopeReader);
    return { columnName, bloomFilters: filters };
  });

  return await Promise.all(promises);
};

module.exports = {
  siftAllOffsetByColumn,
  readFilterBlocksFrom,
  parseBloomFilterOffsets,
};
