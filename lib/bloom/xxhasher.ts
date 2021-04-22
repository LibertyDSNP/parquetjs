const xxhash = require("xxhash")

class XxHasher {
    static hash64(value: any): string {
        if (value instanceof Buffer) return xxhash.hash64(value)
        if (typeof value === 'string') return xxhash.hash64(Buffer.from(value), 0x0, 'hex')
        return xxhash.hash64(Buffer.from(JSON.stringify(value)),0x0, 'hex')
    }
}

export default XxHasher
