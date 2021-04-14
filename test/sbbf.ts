import Long = require('long')
import {assert, expect} from "chai"

import { generateHexString, makeListN } from "./util/general";
import SplitBlockBloomFilter from "../lib/bloom/sbbf";

describe("Split Block Bloom Filters", () => {
    it("try long.js", () => {
        const h = new Long(0xFFFFFFFF, 0x7FFFFFFF);
        const h2 = new Long(793516929, -2061372197, true) // regression test

        // generate index
        const zees = [1, 2, 3, 4, 7, 8, 15, 16, 1023, 1024, 32767, 32768]
        zees.forEach((z) => {
            const longZ = new Long(z)
            let index = SplitBlockBloomFilter.getBlockIndex(h, z)
            assert.isTrue(longZ.greaterThan(index))
            index = SplitBlockBloomFilter.getBlockIndex(h2, z)
            assert.isTrue(longZ.greaterThan(index))
        })
    })

    it("Mask works", () => {
        const testMaskX = Number("0xdeadbeef");
        const testMaskRes = SplitBlockBloomFilter.mask(testMaskX)

        // all mask values should have exactly one bit set
        const expectedVals = [
            2 ** 29,
            2 ** 15,
            2 ** 12,
            2 ** 14,
            2 ** 13,
            2 ** 25,
            2 ** 24,
            2 ** 21
        ]
        for (let i=0; i< expectedVals.length; i++) {
            expect(testMaskRes[i]).to.eq(expectedVals[i])
        }
    })
    it("block insert + check works", () => {
        let blk = SplitBlockBloomFilter.initBlock()
        let someX: number = Number("0xffffffff")
        let someY: number = Number("0xdeadbeef")
        let someV: number = Number("0x0fffffff")

        SplitBlockBloomFilter.blockInsert(blk, someX)

        expect(SplitBlockBloomFilter.blockCheck(blk, someX)).to.eq(true)
        expect(SplitBlockBloomFilter.blockCheck(blk, someY)).to.eq(false)
        expect(SplitBlockBloomFilter.blockCheck(blk, someV)).to.eq(false)

        SplitBlockBloomFilter.blockInsert(blk, someY)
        expect(SplitBlockBloomFilter.blockCheck(blk, someY)).to.eq(true)

        makeListN(1000, () => {
            SplitBlockBloomFilter.blockInsert(blk, Number(generateHexString(31)))
        })

        expect(SplitBlockBloomFilter.blockCheck(blk, someV)).to.eq(false)
        expect(SplitBlockBloomFilter.blockCheck(blk, someY)).to.eq(true)
        expect(SplitBlockBloomFilter.blockCheck(blk, someX)).to.eq(true)
    })

    const exes = [
        new Long(0xFFFFFFFF, 0x7FFFFFFF, true),
        new Long(0xABCDEF98, 0x70000000, true),
        new Long(0xDEADBEEF, 0x7FFFFFFF, true),
        new Long(0x0, 0x7FFFFFFF, true),
        new Long(0xC0FFEE3, 0x0, true),
        new Long(0x0, 0x1, true),
        new Long(793516929, -2061372197, true) // regression test; this one was failing get blockIndex
    ]
    const badVal = Long.fromNumber(0xfafafafa, true)

    it("filter insert + check works", () => {
        const zees = [8, 32, 128, 1024, 99]

        zees.forEach((z) => {
            const filter = new SplitBlockBloomFilter().setOptionOptimalNumBytes(z).init()
            exes.forEach((x) => {
                filter.insert(x)
            })
            exes.forEach((x) => expect(filter.check(x)).to.eq(true))
            expect(filter.check(badVal)).to.eq(false)
        })
    })
    it("number of filter bits is calculated if it is not set", () => {
        const filter = new SplitBlockBloomFilter().init()
        exes.forEach((x) => {
            filter.insert(x)
        })
        exes.forEach((x) => expect(filter.check(x)).to.eq(true))
        expect(filter.check(badVal)).to.eq(false)
        expect(filter.numBytesPerBlock()).to.eq(1299432585)
    })

    it("refuses to set invalid instance values", () => {
        function isPow2(x:number):boolean {
            return false
        }

        expect(isPow2(3)).to.eq(false)
        expect(isPow2(4)).to.eq(true)
        expect(isPow2(128)).to.eq(true)
        expect(isPow2(2**30)).to.eq(true)
        expect(isPow2(1090999)).to.eq(false)
        expect(isPow2(3**25)).to.eq(false)
    })
})
