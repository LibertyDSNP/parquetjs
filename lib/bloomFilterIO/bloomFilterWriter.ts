import * as parquet_util from '../util';
import parquet_thrift from '../../gen-nodejs/parquet_types';
import SplitBlockBloomFilter from '../bloom/sbbf';

import { Block } from '../declare';
import Int64 from 'node-int64';

export interface createSBBFParams {
  numFilterBytes?: number;
  falsePositiveRate?: number;
  numDistinct?: number;
  column?: any;
}

export const createSBBF = (params: createSBBFParams): SplitBlockBloomFilter => {
  const { numFilterBytes, falsePositiveRate, numDistinct } = params;

  const bloomFilter = new SplitBlockBloomFilter();

  const hasOptions = numFilterBytes || falsePositiveRate || numDistinct;

  if (!hasOptions) return bloomFilter.init();

  if (numFilterBytes) return bloomFilter.setOptionNumFilterBytes(numFilterBytes).init();

  if (falsePositiveRate) bloomFilter.setOptionFalsePositiveRate(falsePositiveRate);

  if (numDistinct) bloomFilter.setOptionNumDistinct(numDistinct);

  return bloomFilter.init();
};

const serializeFilterBlocks = (blocks: Block[]): Buffer =>
  Buffer.concat(blocks.map((block) => Buffer.from(block.buffer)));

const buildFilterHeader = (numBytes: number) => {
  const bloomFilterHeader = new parquet_thrift.BloomFilterHeader();
  bloomFilterHeader.numBytes = numBytes;
  bloomFilterHeader.algorithm = new parquet_thrift.BloomFilterAlgorithm();
  bloomFilterHeader.hash = new parquet_thrift.BloomFilterHash();
  bloomFilterHeader.compression = new parquet_thrift.BloomFilterCompression();

  return bloomFilterHeader;
};

export const serializeFilterHeaders = (numberOfBytes: number) => {
  const bloomFilterHeader = buildFilterHeader(numberOfBytes);

  return parquet_util.serializeThrift(bloomFilterHeader);
};

interface serializeFilterDataParams {
  filterBlocks: Block[];
  filterByteSize: number;
}

export const serializeFilterData = (params: serializeFilterDataParams) => {
  const serializedFilterBlocks = serializeFilterBlocks(params.filterBlocks);
  const serializedFilterHeaders = serializeFilterHeaders(params.filterByteSize);

  return Buffer.concat([serializedFilterHeaders, serializedFilterBlocks]);
};

export const setFilterOffset = (column: parquet_thrift.ColumnChunk, offset: Int64) => {
  column.meta_data!.bloom_filter_offset = parquet_util.cloneInteger(offset);
};

export const getSerializedBloomFilterData = (
  splitBlockBloomFilter: InstanceType<typeof SplitBlockBloomFilter>
): Buffer => {
  const filterBlocks = splitBlockBloomFilter.getFilter();
  const filterByteSize = splitBlockBloomFilter.getNumFilterBytes();

  return serializeFilterData({ filterBlocks, filterByteSize });
};
