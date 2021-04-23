const parquet_thrift = require('../gen-nodejs/parquet_types');
const parquet_util = require('./util');
const SplitBlockBloomFilter = require('../dist/lib/bloom/sbbf').default;

const buildFilterBlocks = ({ numFilterBytes, falsePositiveRate, numDistinct }) => {
  const splitblockBloomFilter = new SplitBlockBloomFilter();

  const hasOptions = numFilterBytes || falsePositiveRate || numDistinct;

  if (!hasOptions) return splitblockBloomFilter.init();

  if (numFilterBytes) splitblockBloomFilter.setOptionNumFilterBytes(numFilterBytes).init();

  if (falsePositiveRate) splitblockBloomFilter.setOptionFalsePositiveRate(falsePositiveRate);
  
  if (numDistinct) splitblockBloomFilter.setOptionNumDistinct(numDistinct);

  return splitblockBloomFilter.init();
}

const serializeFilterBlocks = blocks => 
  Buffer.concat(blocks.map(block => Buffer.from(block.buffer)));

const buildFilterHeader = numBytes => {
  const bloomFilterHeader = new parquet_thrift.BloomFilterHeader();
  bloomFilterHeader.numBytes = numBytes;
  bloomFilterHeader.algorithm = new parquet_thrift.BloomFilterAlgorithm();
  bloomFilterHeader.algorithm.BLOCK = new parquet_thrift.SplitBlockAlgorithm();
  bloomFilterHeader.hash = new parquet_thrift.BloomFilterHash();
  bloomFilterHeader.compression = new parquet_thrift.BloomFilterCompression();

  return bloomFilterHeader;
}

const serializeFilterHeaders = numberOfBytes => {
  const bloomFilterHeader = buildFilterHeader(numberOfBytes);

  return parquet_util.serializeThrift(bloomFilterHeader);
}

const serializeFilterData = filterBlocks => {
  const serializedFilterBlocks = serializeFilterBlocks(filterBlocks);
  const serilizedFilterHeaders = serializeFilterHeaders(filterBlocks.length);

  return Buffer.concat([serilizedFilterHeaders, serializedFilterBlocks]);
}

const setFilterOffset = (column, offset) => {
  column.meta_data.bloom_filter_offset = offset;
}

module.exports = {
  serializeFilterData,
  setFilterOffset,
  buildFilterBlocks
}
