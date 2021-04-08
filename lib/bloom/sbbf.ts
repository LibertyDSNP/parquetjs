import Long = require('long')

declare type Block = Uint32Array

const salt: Array<number> = [
    0x47b6137b,
    0x44974d91,
    0x8824ad5b,
    0xa2b7289d,
    0x705495c7,
    0x2df1424b,
    0x9efc4947,
    0x5c6bfb31
];

function initBlock(): Block {
    return Uint32Array.from(Array(8).fill(0))
}

function initSplitBlocks(z:number): Array<Block> {
    return Array(z).fill(initBlock())
}

function mask(x: number): Block {
    let result: Block = initBlock()
    for (let i = 0; i < 8; i++) {
        const y = x * salt[i]
        result[i] = result[i] | (1 << (y >>> 27))
    }
    return result
}

function blockInsert(b: Block, x: number) {
    const masked: Block = mask(x)
    for (let i = 0; i < 8; i++) {
        for (let j = 0; j < 31; j++) {
            const isSet = masked[i] & (2 ** j)
            if (isSet) {
                b[i] = b[i] | (2 ** j)
            }
        }
    }
}

function blockCheck(b: Block, x: number) {
    const masked: Block = mask(x)
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

function getBlockIndex(h: Long, z: number): number {
    const zLong = Long.fromNumber(z, true)
    const h_top_bits = Long.fromNumber(h.getHighBitsUnsigned(), true);
    return h_top_bits.mul(zLong).shiftRightUnsigned(32).getLowBitsUnsigned();
}

function filterInsert(filter: Array<Block>, x: Long):void {
    const i = getBlockIndex(x, filter.length)
    const block:Block = filter[i];
    blockInsert(block, x.getLowBitsUnsigned());
}

function filterCheck(filter: Array<Block>, x:Long): boolean  {
    const i = getBlockIndex(x, filter.length)
    const block:Block = filter[i];

    return blockCheck(block,  x.getLowBitsUnsigned());
}

module.exports = {
    blockCheck: blockCheck,
    blockInsert: blockInsert,
    getBlockIndex: getBlockIndex,
    filterCheck: filterCheck,
    filterInsert: filterInsert,
    initBlock: initBlock,
    initSplitBlocks: initSplitBlocks,
    mask: mask
}
