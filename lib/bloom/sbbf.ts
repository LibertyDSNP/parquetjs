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

    // How many bits are in a single block:
    // - Blocks are UInt32 arrays
    // - There are 8 UInt32 words in each block.
    private static WORDS_PER_BLOCK = 8
    private static WORD_SIZE = 32

    // How many bits are in a single block: 256
    private static BITS_SET_PER_BLOCK: number = SplitBlockBloomFilter.WORDS_PER_BLOCK * SplitBlockBloomFilter.WORD_SIZE

    // Default number of blocks in a Split Block Bloom filter (SBBF)
    private static NUMBER_OF_BLOCKS: number = 32

    // The lower bound of SBBF size in bytes.
    // Currently this is 1024
    private static LOWER_BOUND_BYTES = SplitBlockBloomFilter.NUMBER_OF_BLOCKS * SplitBlockBloomFilter.BITS_SET_PER_BLOCK / 8;

    // The upper bound of SBBF size, set to default row group size in bytes.
    // Note that the subsquent requirements for an effective bloom filter on a row group this size would mean this
    // is unacceptably large for a lightweight client application.
    public static UPPER_BOUND_BYTES = 128 * 1024 * 1024;

    public static DEFAULT_FALSE_POSITIVE_RATE = 0.01

    static initBlock = function (): Block {
        return Uint32Array.from(Array(SplitBlockBloomFilter.WORDS_PER_BLOCK).fill(0))
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
     * Using a Bloom filter calculator, the upper bound is far too large for client applications.
     * More reasonable settings, if what's desired is:
     *   - n = Upper bound row group size = 32kB
     *   - p = .001, 1 in 1000 chance of false positive
     *   - k = 8, number of hash functions
     *
     *   means m, number of bits in the filter needs to be:
     *   - m = 467447 (57.06KiB) , which means
     *   - m/BITS_SET_PER_BLOCK = m/256 = just over 1825 blocks.
     *
     * @param numDistinct: The number of distinct values.
     * @param falsePositiveProbability: The false positive probability.
     *
     * @return optimal number of bits of given n and p.
     */
    static optimalNumOfBlocks(numDistinct: number, falsePositiveProbability: number): number {
        const foo = Math.pow(falsePositiveProbability, 1.0 / 8);
        let something = Math.log(1 - foo);
        const m = numDistinct * -8 / something

        let numBits = (m + SplitBlockBloomFilter.NUMBER_OF_BLOCKS - 1) & (~SplitBlockBloomFilter.NUMBER_OF_BLOCKS);

        // Handle overflow:
        const upperShiftL3 = SplitBlockBloomFilter.UPPER_BOUND_BYTES << 3
        if (m > upperShiftL3 || m < 0 ) {
            numBits = upperShiftL3;
        }
        // Round numBits up to (k * NUMBER_OF_BLOCKS)?
        const lowerBoundShiftL3 = SplitBlockBloomFilter.LOWER_BOUND_BYTES << 3
        if (numBits < lowerBoundShiftL3 ) {
            numBits = lowerBoundShiftL3;
        }

        return numBits / this.BITS_SET_PER_BLOCK;
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
        for (let i = 0; i < result.length; i++) {
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
        for (let i = 0; i < masked.length; i++) {
            for (let j = 0; j < this.WORD_SIZE; j++) {
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
        for (let i = 0; i < masked.length; i++) {
            for (let j = 0; j < this.WORD_SIZE; j++) {
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
    private desiredFalsePositiveRate: number = SplitBlockBloomFilter.DEFAULT_FALSE_POSITIVE_RATE
    private numBlocks: number = 0
    private numDistinctValues: number = SplitBlockBloomFilter.UPPER_BOUND_BYTES
    // hashStrategy =  new parquet_thrift.BloomFilterHash(parquet_thrift.XxHash)

    private isInitialized(): boolean { return this.splitBlockFilter.length > 0 }

    numFilterBlocks(): number { return this.numBlocks }
    optFalsePositiveRate(): number { return this.desiredFalsePositiveRate }
    optNumDistinct(): number { return this.numDistinctValues }
    filter(): Array<Block> { return this.splitBlockFilter }

    // numFilterBytes
    // default:  256 * 4
    optNumFilterBytes(): number {
        return this.numBlocks * SplitBlockBloomFilter.BITS_SET_PER_BLOCK >>> 3
    }

    /**
     * setOptionFalsePositiveRate: set the desired false positive percentage for this Bloom filter.
     * defaults to SplitBlockBLoomFilter.DEFAULT_FALSE_POSITIVE_RATE
     * This function does nothing if the filter has already been allocated.
     * @param proportion: number, between 0.0 and 1.0, exclusive
     */
    setOptionFalsePositiveRate(proportion: number): SplitBlockBloomFilter {
        if (this.isInitialized()) {
            console.error("filter already initialized. options may no longer be changed.")
            return this
        }
        if (proportion <= 0.0 || proportion >= 1.0) {
            console.error("falsePositiveProbability. Must be < 1.0 and > 0.0")
            return this
        }
        this.desiredFalsePositiveRate = proportion
        return this
    }

    /**
     * setOptionNumDistinct: set the number of expected distinct values for the filter.
     *  this should generally be <= to the row group size. Defaults to
     *  SplitBlockBloomFilter.UPPER_BOUND_BYTES
     *  This function does nothing if the filter has already been allocated.
     * @param numDistinct
     */
    setOptionNumDistinct(numDistinct: number): SplitBlockBloomFilter {
        if (this.isInitialized()) {
            console.error("filter already initialized. options may no longer be changed.")
            return this
        }
        if (numDistinct <= 0 || numDistinct > SplitBlockBloomFilter.UPPER_BOUND_BYTES) {
            console.error(`invalid numDistinct. Must be > 0 and < ${SplitBlockBloomFilter.UPPER_BOUND_BYTES}`)
            return this
        }
        this.numDistinctValues = numDistinct
        return this
    }


    /**
     * nextPwr2: return the next highest power of 2 above v
     * see  https://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
     * @param v: the number to increase
     * @returns the new number
     */
    private static nextPwr2(v:number): number {
        v--;
        v |= v >> 1;
        v |= v >> 2;
        v |= v >> 4;
        v |= v >> 8;
        v |= v >> 16;
        v++;
        return v
    }

    /**
     * setOptionNumBytes: set the bytes for this Bloom filter. Set this if you don't want an
     * optimal value calculated for you.  Rounds up to nearest power of 2
     * This function does nothing if the filter has already been allocated.
     *
     * @param numBytes: number, the desired bit size.
     */
    setOptionNumFilterBytes(numBytes: number): SplitBlockBloomFilter {
        if (this.isInitialized()) {
            console.error("filter already initialized. options may no longer be changed.")
            return this
        }
        if (numBytes < SplitBlockBloomFilter.LOWER_BOUND_BYTES || numBytes > SplitBlockBloomFilter.UPPER_BOUND_BYTES) {
            console.error(`invalid numBits. Must be > ${SplitBlockBloomFilter.LOWER_BOUND_BYTES} and < ${SplitBlockBloomFilter.UPPER_BOUND_BYTES}`)
            return this
        }
        // numBlocks = Bytes * 8b/Byte * 1Block/256b
        this.numBlocks =  SplitBlockBloomFilter.nextPwr2(numBytes) * 8 / SplitBlockBloomFilter.BITS_SET_PER_BLOCK
        return this
    }

    // TODO: include an option to set hash strategy later even though there's only one valid one

    // is numBits the correct value to use?
    /**
     * initFilter: initialize the Bloom filter using the options previously provided.
     * If numBlocks has not been calculated and set via setOptionNumBytes, we calculate
     * the optimal filter size based on number of distinct values and
     * percent false positive rate. See setOptionNumDistinct and setOptionFalsePositiveRate
     *
     * Repeated calls to init do nothing to avoid multiple memory allocations or
     * accidental loss of filters.
     */
    init(): SplitBlockBloomFilter {
        if (this.isInitialized()) {
            console.error("filter already initialized.")
            return this
        }
        if (this.numBlocks === 0) {
            this.numBlocks = SplitBlockBloomFilter.optimalNumOfBlocks(this.numDistinctValues, this.desiredFalsePositiveRate) >>> 3
        }

        this.splitBlockFilter = Array(this.numBlocks).fill(SplitBlockBloomFilter.initBlock())
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
