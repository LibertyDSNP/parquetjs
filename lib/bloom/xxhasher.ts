import xxhash from "xxhash-wasm";
import Long from "long"

type HasherFunc = (input: string, seedHigh?: number, seedLow?: number) => string

/**
 * @class XxHasher
 *
 * @description  Simple wrapper for xxhash package that makes educated guesses to convert
 * Parquet Type analogs in JavaScript to strings for creating 64 bit hashes.  Hash seed = 0 per
 * [Parquet specification](https://github.com/apache/parquet-format/blob/master/BloomFilter.md).
 *
 * See also:
 * [xxHash spec](https://github.com/Cyan4973/xxHash/blob/v0.7.0/doc/xxhash_spec.md)
 */
class XxHasher {
    hasher: HasherFunc | undefined

    private async hashit(value: string): Promise<string> {
        if (this.hasher === undefined) {
            const {h64} = await xxhash()
            this.hasher = h64
        }
        // @ts-ignore
        return this.hasher(value)
    }

    /**
     * @function hash64
     * @description attempts to create a hash for certain data types.
     * @return the 64 big XXHash as a string
     * @param value one of n, throw an error.
     */
    async hash64(value: any): Promise<string> {
        if (typeof value === 'string') return this.hashit(value)
        if (value instanceof Buffer ||
            value instanceof Uint8Array ||
            value instanceof Long ||
            typeof value === 'boolean' ||
            typeof value === 'number' ||
            typeof value === 'bigint') {
            return this.hashit(value.toString())
        }
        throw new Error("unsupported type: " + value)
    }
}

export = XxHasher;
