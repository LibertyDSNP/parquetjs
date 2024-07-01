import xxhash from 'xxhash-wasm';
import Long from 'long';

/**
 * @class XxHasher
 *
 * @description  Simple wrapper for xxhash-wasm package that converts
 * Parquet Type analogs in JavaScript to strings for creating 64 bit hashes.  Hash seed = 0 per
 * [Parquet specification](https://github.com/apache/parquet-format/blob/master/BloomFilter.md).
 *
 * See also:
 * [xxHash spec](https://github.com/Cyan4973/xxHash/blob/v0.7.0/doc/xxhash_spec.md)
 */
export default class XxHasher {
  private static h64 = xxhash().then((x) => x.h64ToString);

  private async hashIt(value: string): Promise<string> {
    return (await XxHasher.h64)(value);
  }

  /**
   * @function hash64
   * @description creates a hash for certain data types. All data is converted using toString()
   * prior to hashing.
   * @return the 64 big XXHash as a hex-encoded string.
   * @param value, must be of type string, Buffer, Uint8Array, Long, boolean, number, or bigint
   */
  async hash64(value: any): Promise<string> {
    if (typeof value === 'string') return this.hashIt(value);
    if (
      value instanceof Buffer ||
      value instanceof Uint8Array ||
      value instanceof Long ||
      typeof value === 'boolean' ||
      typeof value === 'number' ||
      typeof value === 'bigint'
    ) {
      return this.hashIt(value.toString());
    }
    throw new Error('unsupported type: ' + value);
  }
}
