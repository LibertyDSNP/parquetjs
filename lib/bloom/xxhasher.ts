const xxhash = require("xxhash");
import Long from "long"

const HASH_SEED = 0x0
type SupportedType = Buffer | Uint8Array | Long | string | number | bigint | boolean

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
  private static hashWithToString(value: any): string {
    return xxhash.hash64(Buffer.from(value.toString()), HASH_SEED, 'hex')
  }

  private static hash64Buffer(value: Buffer): string {
    return xxhash.hash64(value, HASH_SEED, 'hex')
  }

  private static hash64Bytes(value: string | Uint8Array): string {
    return xxhash.hash64(Buffer.from(value), HASH_SEED, 'hex')
  }


  /**
   * @function hash64
   * @description attempts to create a hash for certain data types.
   * @return the 64 big XXHash as a string
   * @param value one of n, throw an error.
   */
  static hash64(value: SupportedType): string {
    if (value instanceof Buffer) return this.hash64Buffer(value)

    if (value instanceof Uint8Array) return this.hash64Bytes(value)

    if (value instanceof Long) return this.hashWithToString(value)

    switch (typeof value) {
      case 'string':
        return this.hash64Bytes(value)
      case 'number': // FLOAT, DOUBLE, INT32?
      case 'bigint':
      case 'boolean':
        return this.hashWithToString(value)
      default:
        throw new Error("unsupported type: " + value)
    }
  }
}

export default XxHasher
