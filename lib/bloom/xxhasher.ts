const xxhash = require("xxhash")

const HASH_SEED = 0x0

class XxHasher {
    static hash64(value: any): string {
        if (value instanceof Set || value instanceof Map) {
            throw new Error("stringify Set or Map first")
        }

        if (value instanceof Buffer) return xxhash.hash64(value, HASH_SEED, 'hex')
        if (typeof value === 'string') return xxhash.hash64(Buffer.from(value), HASH_SEED, 'hex')
        if (typeof value === 'bigint') return xxhash.hash64(Buffer.from(value.toString()), HASH_SEED, 'hex')
        else return xxhash.hash64(Buffer.from(JSON.stringify(value, null, 0)), HASH_SEED, 'hex')
    }
}

export default XxHasher
