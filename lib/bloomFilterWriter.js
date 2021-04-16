const parquet_thrift = require('../gen-nodejs/parquet_types')
const parquet_util = require('./util')

const serializeFilterBlocks = (blocks) => 
  Buffer.concat(blocks.map(block => Buffer.from(block.buffer)));

const buildBloomFilterHeader = () => {
  const bloomFilterHeader = new parquet_thrift.BloomFilterHeader();
  bloomFilterHeader.numBytes = 128 //blocks.length;; // option data;
  bloomFilterHeader.algorithm = new parquet_thrift.BloomFilterAlgorithm();
  bloomFilterHeader.algorithm.BLOCK = new parquet_thrift.SplitBlockAlgorithm();
  bloomFilterHeader.hash = new parquet_thrift.BloomFilterHash()
  bloomFilterHeader.compression = new parquet_thrift.BloomFilterCompression();

  return bloomFilterHeader;
}

const serializeBloomHeaders = () => {
  const bloomFilterHeader = buildBloomFilterHeader();
  return parquet_util.serializeThrift(bloomFilterHeader);
}

const serializeBloomFilterData = filterBlocks => {
    const serializedFilterBlocks = serializeFilterBlocks(filterBlocks);
    const serilizedBloomFilterHeaders = serializeBloomHeaders();
    return Buffer.concat([serilizedBloomFilterHeaders, serializedFilterBlocks]);
}

const setBloomFilterOffset = (column, offset) => {
  column.meta_data.bloom_filter_offset = offset;
}

module.exports = {
  serializeFilterBlocks,
  buildBloomFilterHeader,
  serializeBloomHeaders,
  serializeBloomFilterData,
  setBloomFilterOffset
}
