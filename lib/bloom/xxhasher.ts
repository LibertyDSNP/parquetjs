const xxhash = require("xxhash")
import Long = require('long')


class XxHasher {
    static hash64(value: any): string {
        if (typeof value === 'string') return xxhash.hash64(Buffer.from(value), 0x0, 'hex')
        if (Buffer.isBuffer(value)) return xxhash.hash64(value, 0x0, 'hex');
        return xxhash.hash64(Buffer.from(JSON.stringify(value)),0x0, 'hex')
    }
}

export default XxHasher
