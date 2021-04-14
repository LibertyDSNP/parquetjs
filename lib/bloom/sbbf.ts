import Long = require('long')
// const parquet_thrift = require('../gen-nodejs/parquet_types')

type Block = Uint32Array

class SplitBlockBloomFilter {
    private static salt: Array<number> = [
        0x47b6137b,
        0x44974d91,
        0x8824ad5b,
        0xa2b7289d,
        0x705495c7,
        0x2df1424b,
        0x9efc4947,
        0x5c6bfb31
    ];

    // Default bits in a Bloom filter block,
    // aka "z"?
    private static BITS_PER_BLOCK: number = 64

    // The lower bound of bloom filter size.
    // aka ???
    private static LOWER_BOUND_BYTES = 32;

    // The upper bound of bloom filter size, set to default row group size.
    public static UPPER_BOUND_BYTES = 128 * 1024 * 1024;

    public static DEFAULT_FALSE_POSITIVE_RATE = 0.01

    private static BITS_SET_PER_BLOCK: number = 8

    static initBlock = function (): Block {
        return Uint32Array.from(Array(SplitBlockBloomFilter.BITS_SET_PER_BLOCK).fill(0))
    }

    /**
     * getBlockIndex: get a block index to insert a hash value for
     * @param h: the hash from which to derive a block index (?)
     * @param z: the number of blocks in the filter
     *
     * @return a number from 0 -> z-1
     */
    static getBlockIndex(h: Long, z: number): number {
        const zLong = Long.fromNumber(z, true)
        const h_top_bits = Long.fromNumber(h.getHighBitsUnsigned(), true);
        return h_top_bits.mul(zLong).shiftRightUnsigned(32).getLowBitsUnsigned();
    }

    /**
     * Calculate optimal size according to the number of distinct values and false positive probability.
     *
     * @param numDistinct: The number of distinct values.
     * @param falsePositiveProbability: The false positive probability.
     *
     * @return optimal number of bits of given n and p.
     */
    static optimalNumOfBits(numDistinct: number, falsePositiveProbability: number): number {
        const m = numDistinct * -8 / Math.log(1 - Math.pow(falsePositiveProbability, 1.0 / 8))

        let numBits = (m + SplitBlockBloomFilter.BITS_PER_BLOCK - 1) & (~SplitBlockBloomFilter.BITS_PER_BLOCK);

        // Handle overflow:
        const upperShiftL3 = SplitBlockBloomFilter.UPPER_BOUND_BYTES << 3
        if (m > upperShiftL3 || m < 0 ) {
            numBits = upperShiftL3;
        }
        // Round numBits up to (k * BITS_PER_BLOCK)
        const lowerBoundShiftL3 = SplitBlockBloomFilter.LOWER_BOUND_BYTES << 3
        if (numBits < lowerBoundShiftL3 ) {
            numBits = lowerBoundShiftL3;
        }

        return numBits;
    }

    /**
     * mask: generate a mask block for a bloom filter block
     * @param hashValue: the hash value to generate the mask from
     * @private
     *
     * @return the mask Block
     */
    static mask(hashValue: number): Block {
        let result: Block = SplitBlockBloomFilter.initBlock()
        for (let i = 0; i < 8; i++) {
            const y = hashValue * SplitBlockBloomFilter.salt[i]
            result[i] = result[i] | (1 << (y >>> 27))
        }
        return result
    }

    /**
     * blockInsert: insert a hash into a Bloom filter Block
     * @param b: the block to flip a bit for: is changed
     * @param hashValue: the hash value to insert into b
     * @private
     *
     * @return void
     */
    // FIXME: hashValue should be a Long val  ?
    // TODO: Make sure the value is preserved; I think it's passed by ref
    static blockInsert(b: Block, hashValue: number): void {
        const masked: Block = this.mask(hashValue)
        for (let i = 0; i < 8; i++) {
            for (let j = 0; j < 31; j++) {
                const isSet = masked[i] & (2 ** j)
                if (isSet) {
                    b[i] = b[i] | (2 ** j)
                }
            }
        }
    }

    /**
     * blockCheck: check if a hashValue exists for this filter
     * @param b: the block to check for inclusion
     * @param hashValue: the hash to check for  should be long
     * @private
     *
     * @return true if hashed item is __probably__ in the data set represented by this filter
     * @return false if it is __definitely not__ in the data set.
     */
    // FIXME: hashValue should be a Long val   ?
    static blockCheck(b: Block, hashValue: number): boolean {
        const masked: Block = this.mask(hashValue)
        for (let i = 0; i < 7; i++) {
            for (let j = 0; j < 31; j++) {
                const isSet = masked[i] & (2 ** j)
                if (isSet) {
                    const match = b[i] & (2 ** j)
                    if (!match) {
                        return false
                    }
                }
            }
        }
        return true
    }

    /**
     * Instance
     */

    private splitBlockFilter: Array<Block> = []
    private falsePostiveRate: number = SplitBlockBloomFilter.DEFAULT_FALSE_POSITIVE_RATE
    private numBytes: number = SplitBlockBloomFilter.BITS_PER_BLOCK
    private numDistinct: number = SplitBlockBloomFilter.UPPER_BOUND_BYTES
    // hashStrategy =  new parquet_thrift.BloomFilterHash(parquet_thrift.XxHash)

    filter(): Array<Block> { return this.splitBlockFilter }
    numBytesPerBlock(): number { return this.numBytes }
    numDistinctExpected(): number { return this.numDistinct }

    /**
     * setOptionFalsePositiveRate: set the desired false positive percentage for this Bloom filter.
     * defaults to SplitBlockBLoomFilter.DEFAULT_FALSE_POSITIVE_RATE
     * @param proportion: number, between 0.0 and 1.0, exclusive
     */
    setOptionFalsePositiveRate(proportion: number): SplitBlockBloomFilter {
        if (proportion <= 0.0 || proportion >= 1.0) {
            console.error("refusing to set falsePositiveProbability. Must be < 1.0 and > 0.0")
            return this
        }
        this.falsePostiveRate = proportion
        return this
    }

    /**
     * setOptionNumDistinct: set the number of expected distinct values for the filter.
     *  this should generally be <= to the row group size. Defaults to
     *  SplitBlockBloomFilter.UPPER_BOUND_BYTES
     * @param numDistinct
     */
    setOptionNumDistinct(numDistinct: number): SplitBlockBloomFilter {
        if (numDistinct <= 0 || numDistinct > SplitBlockBloomFilter.UPPER_BOUND_BYTES) {
            console.error(`refusing to set invalid numDistinct. Must be > 0 and < ${SplitBlockBloomFilter.UPPER_BOUND_BYTES}`)
            return this
        }
        this.numDistinct = numDistinct
        return this
    }

    /**
     * setOptionOptimalNumBytes: set the bytes for this Bloom filter. Set this if you don't want an
     * optimal value calculated for you.
     * TODO: Will round up to nearest power of 2.
     *
     * @param numBits: number, the desired bit size.
     */
    setOptionOptimalNumBytes(numBytes: number): SplitBlockBloomFilter {
        if (numBytes<=0 || numBytes > SplitBlockBloomFilter.UPPER_BOUND_BYTES) {
            console.error(`refusing to set invalid numBits. Must be > 0 and < ${SplitBlockBloomFilter.UPPER_BOUND_BYTES}`)
            return this
        }
        // if ((numBytes & (numBytes - 1)) != 0) {
        //     numBytes = Integer.highestOneBit(numBytes) << 1;
        // }
        this.numBytes = numBytes
        return this
    }

    // TODO: include an option to set hash strategy later even though there's only one valid one

    // is numBits the correct value to use?
    /**
     * initFilter: initialize the Bloom filter using the options previously provided.
     * If the number of bits has NOT been set, the optimal number will be calculated.
     */
    init(): SplitBlockBloomFilter {
        if (this.numBytes === 0) {
            this.numBytes = SplitBlockBloomFilter.optimalNumOfBits(this.numDistinct, this.falsePostiveRate) >>> 3
        }
        this.splitBlockFilter = Array(this.numBytes).fill(SplitBlockBloomFilter.initBlock())
        return this
    }

    //
    // hash(value: any): Long {
    //     return this.hashStrategy(value)
    // }

    /**
     * insert: add a hash value to this filter
     * @param hashValue: Long, the hash value to add
     */
    insert(hashValue: Long): void {
        if (!hashValue.unsigned) throw new Error("hashValue must be an unsigned Long")
        const i = SplitBlockBloomFilter.getBlockIndex(hashValue, this.splitBlockFilter.length)
        SplitBlockBloomFilter.blockInsert(this.splitBlockFilter[i], hashValue.getLowBitsUnsigned());
    }

    /**
     * check: blockCheck: check if a hashValue exists for this filter
     * @param hashValue: Long,  the hash value to check for
     *
     * @return true if hashed item is __probably__ in the data set represented by this filter
     * @return false if it is __definitely not__ in the data set.
     */
    check(hashValue: Long): boolean {
        const i = SplitBlockBloomFilter.getBlockIndex(hashValue, this.splitBlockFilter.length)
        return SplitBlockBloomFilter.blockCheck(this.splitBlockFilter[i], hashValue.getLowBitsUnsigned());
    }
}

export default SplitBlockBloomFilter
