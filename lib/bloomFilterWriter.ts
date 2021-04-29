import SplitBlockBloomFilter from "./bloom/sbbf";
import {ColumnData, Offset, Block} from "./types/types";

const parquet_thrift = require("../gen-nodejs/parquet_types");
const parquet_util = require("./util");

type createSBBFParams = {
  numFilterBytes?: number
  falsePositiveRate?: number
  numDistinct?: number }

export const createSBBF = (params: createSBBFParams): SplitBlockBloomFilter => {
  const {numFilterBytes, falsePositiveRate, numDistinct} = params

  const bloomFilter = new SplitBlockBloomFilter()

  const hasOptions = numFilterBytes || falsePositiveRate || numDistinct;

  if (!hasOptions) return bloomFilter.init();

  if (numFilterBytes)
    return bloomFilter.setOptionNumFilterBytes(numFilterBytes).init();

  if (falsePositiveRate)
  bloomFilter.setOptionFalsePositiveRate(falsePositiveRate);

  if (numDistinct) bloomFilter.setOptionNumDistinct(numDistinct);

  return bloomFilter.init();
};

const serializeFilterBlocks = (blocks: Array<Block>): Buffer =>
  Buffer.concat(blocks.map((block) => Buffer.from(block.buffer)));

const buildFilterHeader = (numBytes: number) => {
  const bloomFilterHeader = new parquet_thrift.BloomFilterHeader();
  bloomFilterHeader.numBytes = numBytes;
  bloomFilterHeader.algorithm = new parquet_thrift.BloomFilterAlgorithm();
  bloomFilterHeader.algorithm.BLOCK = new parquet_thrift.SplitBlockAlgorithm();
  bloomFilterHeader.hash = new parquet_thrift.BloomFilterHash();
  bloomFilterHeader.compression = new parquet_thrift.BloomFilterCompression();

  return bloomFilterHeader;
};

const serializeFilterHeaders = (numberOfBytes: number) => {
  const bloomFilterHeader = buildFilterHeader(numberOfBytes);

  return parquet_util.serializeThrift(bloomFilterHeader);
};

type serializeFilterDataParams = {
  filterBlocks: Array<Block>
  filterByteSize: number
}

const serializeFilterData = (params: serializeFilterDataParams) => {
  const serializedFilterBlocks = serializeFilterBlocks(params.filterBlocks);
  const serializedFilterHeaders = serializeFilterHeaders(params.filterByteSize);

  return Buffer.concat([serializedFilterHeaders, serializedFilterBlocks]);
};

const setFilterOffset = (column: ColumnData, offset: Offset) => {
  column.meta_data.bloom_filter_offset = offset;
};
