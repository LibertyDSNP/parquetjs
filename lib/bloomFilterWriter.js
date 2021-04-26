const parquet_thrift = require("../gen-nodejs/parquet_types");
const parquet_util = require("./util");
const SplitBlockBloomFilter = require("./bloom/sbbf").default;

const createSBBF = ({
  numFilterBytes,
  falsePositiveRate,
  numDistinct,
}) => {
  const sbbf = new SplitBlockBloomFilter();

  const hasOptions = numFilterBytes || falsePositiveRate || numDistinct;

  if (!hasOptions) return sbbf.init();

  if (numFilterBytes)
    return sbbf.setOptionNumFilterBytes(numFilterBytes).init();

  if (falsePositiveRate)
  sbbf.setOptionFalsePositiveRate(falsePositiveRate);

  if (numDistinct) sbbf.setOptionNumDistinct(numDistinct);

  return sbbf.init();
};

const serializeFilterBlocks = (blocks) =>
  Buffer.concat(blocks.map((block) => Buffer.from(block.buffer)));

const buildFilterHeader = (numBytes) => {
  const bloomFilterHeader = new parquet_thrift.BloomFilterHeader();
  bloomFilterHeader.numBytes = numBytes;
  bloomFilterHeader.algorithm = new parquet_thrift.BloomFilterAlgorithm();
  bloomFilterHeader.algorithm.BLOCK = new parquet_thrift.SplitBlockAlgorithm();
  bloomFilterHeader.hash = new parquet_thrift.BloomFilterHash();
  bloomFilterHeader.compression = new parquet_thrift.BloomFilterCompression();

  return bloomFilterHeader;
};

const serializeFilterHeaders = (numberOfBytes) => {
  const bloomFilterHeader = buildFilterHeader(numberOfBytes);

  return parquet_util.serializeThrift(bloomFilterHeader);
};

const serializeFilterData = ({ filterBlocks, filterByteSize }) => {
  const serializedFilterBlocks = serializeFilterBlocks(filterBlocks);
  const serializedFilterHeaders = serializeFilterHeaders(filterByteSize);

  return Buffer.concat([serializedFilterHeaders, serializedFilterBlocks]);
};

const setFilterOffset = (column, offset) => {
  column.meta_data.bloom_filter_offset = offset;
};

module.exports = {
  serializeFilterData,
  setFilterOffset,
  createSBBF,
};
