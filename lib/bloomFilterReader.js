const parquet_util = require("./util");
const parquet_thrift = require("../gen-nodejs/parquet_types");
const sbbf =  require("./bloom/sbbf").default;

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

    const hexPrefix = "0x";
    const bloomFilterOffset = parseInt(
      hexPrefix + bloomFilterOffsetBuffer.toString("hex")
    );

    const columnName = pathInSchema.join("");

    if (columnName in accumulator === false) {
      accumulator[columnName] = [];
    }

    accumulator[columnName].push({
      bloomFilterOffset,
      rowGroup,
    });

    return accumulator;
  }, {});
};

const getBloomFilterHeader = async (bloomFilterOffset, envelopeReader) => {
  const bloomFilterHeaderData = await envelopeReader.read(
    bloomFilterOffset,
    120
  ); // i hardcoded
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
  const blockSize = 4 * 8;
  const {
    bloomFilterHeader,
    sizeOfBloomFilterHeader,
  } = await getBloomFilterHeader(bloomFilterOffset, envelopeReader);

  const { numBytes } = bloomFilterHeader;
  const bitsize = numBytes * blockSize;

  try {
    const offset = bloomFilterOffset + sizeOfBloomFilterHeader;
    const buffer = await envelopeReader.read(offset, bitsize);

    return buffer;
  } catch (e) {
    console.log("porque parquet? error: ", e);
  }
};


const readFilterOffsets = async (bloomOffsets, envelopeReader) => {
  return await Promise.all(
    bloomOffsets.map(async ({ bloomFilterOffset, rowGroup }) => {
      const filterBuffer = await readFilterData(
        bloomFilterOffset,
        envelopeReader
      );
      const filterBlocks = sbbf.from(filterBuffer);

      return {
        rowGroup,
        filterBlocks,
      };
    })
  );
};

const siftAllColumnOffsets = (columnChunks) => {
  const chunks = filterColumnChunksWithBloomFilters(columnChunks);
  const offsets = parseBloomFilterOffsets(chunks);

  return offsets;
};

const readFilterBlocksFor = async (filterOffsets, envelopeReader) => {
  const columnNames = Object.keys(filterOffsets);

  const promises = columnNames.map(async (columnName) => {
    const columnOffsets = filterOffsets[columnName];
    const filters = await readFilterOffsets(columnOffsets, envelopeReader);
    return { columnName, bloomFilters: filters };
  });

  return await Promise.all(promises);
};

module.exports = {
  siftAllColumnOffsets,
  readFilterBlocksFor,
};
