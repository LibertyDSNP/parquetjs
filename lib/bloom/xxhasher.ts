const xxhash = require("xxhash")
import Long from "long"

const HASH_SEED = 0x0

class XxHasher {
    static hashWithToString(value: any): string {
        return xxhash.hash64(Buffer.from(value.toString()), HASH_SEED, 'hex')
    }

    static hash64Buffer(value: Buffer): string {
        return xxhash.hash64(value, HASH_SEED, 'hex')
    }

    static hash64Bytes(value: string | Uint8Array ): string {
        return xxhash.hash64(Buffer.from(value), HASH_SEED, 'hex')
    }

    static hash64(value: any): string {
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
