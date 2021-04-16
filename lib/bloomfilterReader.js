const parquet_util = require('./util')
const parquet_thrift = require('../gen-nodejs/parquet_types')

//TEST
// const sbbf = require("../dist/sbbf");
// const assert = require('assert');
// const Long = require('long');
// var XXHash = require('xxhash');
// const { hash64 } = XXHash;

// const SEED = 0x47b6137b;
// const BUF_SEED = Buffer.alloc(4)
// BUF_SEED.writeUInt32LE(SEED, 0);

// const { filterCheck } = sbbf;

const getBloomFilterColumnChunks = (columns, metadata, envelopeReader) => {
	return metadata
            .row_groups
            .map((rowGroup, index) => 
              columns.map(column => ({
                  rowGroup: index,
                  column: envelopeReader.getColumn(column, rowGroup)
              }))
            )
            .flat()
            .filter(columnData => {
              const {
                  column: {
                  meta_data: { 
                      bloom_filter_offset: { buffer: bloomFilterOffsetBuffer },
                  }
                  }
              } = columnData;
              
              return bloomFilterOffsetBuffer;
            });
}

const parseBloomFilterOffsets = columnChunksMeta => {
	return columnChunksMeta.reduce((accumulator, columnChunk) => {
		const {
			column: {
					meta_data: { 
						bloom_filter_offset: { buffer: bloomFilterOffsetBuffer },
						path_in_schema: pathInSchema
				}
			},
			rowGroup
		} = columnChunk;
			
		const hexPrefix = '0x';
		const bloomFilterOffset = 
			parseInt(hexPrefix + bloomFilterOffsetBuffer.toString('hex'));

		const columnName = pathInSchema.join('');
	
		if (columnName in accumulator === false) {
			accumulator[columnName] = [];
		}

		accumulator[columnName].push({
      bloomFilterOffset,
      rowGroup,
		})

		return accumulator;
	}, {});
}

const getBloomFilterHeader = async (bloomFilterOffset, envelopeReader) => {
	const bloomFilterHeaderData = await envelopeReader.read(bloomFilterOffset, 120); // i hardcoded 
	const bloomFilterHeader = new parquet_thrift.BloomFilterHeader();
	const sizeOfBloomFilterHeader = parquet_util.decodeThrift(bloomFilterHeader, bloomFilterHeaderData);

	return {
		bloomFilterHeader,
		sizeOfBloomFilterHeader
	};
}

const readBloomFilterData = async (bloomFilterOffset, envelopeReader) => {
	const blockSize = 4 * 8;
	const { bloomFilterHeader, sizeOfBloomFilterHeader }  =
					await getBloomFilterHeader(bloomFilterOffset, envelopeReader)

	const { numBytes } = bloomFilterHeader;
	const bitsize = numBytes * blockSize; 
	
	try {
		const offset = bloomFilterOffset + sizeOfBloomFilterHeader;
		const buffer = await envelopeReader.read(offset, bitsize);

		return buffer
	} catch(e) {
		console.log("porque parquet? error: ", e)
	}
}

const parseBloomFilter = (buffer) => {
	return subarrays(new Uint32Array(buffer.buffer), 8)
}

const getBloomFilterForOffsets = async (bloomOffsets, envelopeReader) => {
	return await Promise.all(bloomOffsets.map(async ({bloomFilterOffset, rowGroup}) => {
		const bloomFilterBuffer = await readBloomFilterData(bloomFilterOffset, envelopeReader);
		const bloomFilterBlocks = parseBloomFilter(bloomFilterBuffer)

		return ({
      rowGroup,
      bloomFilterBlocks,
		})
	}));
}

const subarrays = (typedArray, chunkSize) => {
	const result = [];
	const len = typedArray.length;
	let i = 0;

	for(let i = 0; i < len; i += chunkSize) {
		result.push(typedArray.subarray(i, i + chunkSize));
	}

	return result;
}

module.exports = {
	getBloomFilterForOffsets,
	parseBloomFilterOffsets,
	getBloomFilterColumnChunks
}


    // const names = Object.keys(bloomFilterOffsets);
    // return await Promise.all(names.map(async (name) => {
    //   const columnOffsets = bloomFilterOffsets[name]
    //   const filters = await getBloomFilterForOffsets(columnOffsets)
    //   return {columName: name, bloomFilters: filters };
    // }))

    ///
    // TEST
  //   const value = Buffer.from('apples and banannas');
  //   const hexHash = hash64(value, BUF_SEED);
  //   const longValue = Long.fromBytes(hexHash, true);
  //   console.log('check------true', filterCheck(blocks, longValue));
 
  //   const value2 = Buffer.from('apples');
  //   const hexHash2 = hash64(value2, BUF_SEED);
  //   const longValue2 = Long.fromBytes(hexHash2, true);
  //   console.log('check------false', filterCheck(blocks, longValue2));

  //   const value3 = Buffer.from('oranges');
  //   const hexHash3 = hash64(value3, BUF_SEED);
  //   const longValue3 = Long.fromBytes(hexHash3, true);
  //   console.log('check------true', filterCheck(blocks, longValue3));
//   }