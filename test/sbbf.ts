import Long = require('long')
import {assert, expect} from "chai"

const sbbf  = require("../lib/bloom/sbbf")

import { generateHexString, makeListN } from "./util/general";

describe("Split Block Bloom Filters", () => {
    it("try long.js", () => {
        const h = new Long(0xFFFFFFFF, 0x7FFFFFFF);
        const h2 = new Long(793516929, -2061372197, true) // regression test

        // generate index
        const zees = [1, 2, 3, 4, 7, 8, 15, 16, 1023, 1024, 32767, 32768]
        zees.forEach((z) => {
            const longZ = new Long(z)
            let index = sbbf.getBlockIndex(h, z)
            assert.isTrue(longZ.greaterThan(index))
            index = sbbf.getBlockIndex(h2, z)
            assert.isTrue(longZ.greaterThan(index))
        })
    })

    it("Mask works", () => {
        const testMaskX = Number("0xdeadbeef");
        const testMaskRes = sbbf.mask(testMaskX)

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
        let blk = sbbf.initBlock()
        let someX: number = Number("0xffffffff")
        let someY: number = Number("0xdeadbeef")
        let someV: number = Number("0x0fffffff")

        sbbf.blockInsert(blk, someX)

        expect(sbbf.blockCheck(blk, someX)).to.eq(true)
        expect(sbbf.blockCheck(blk, someY)).to.eq(false)
        expect(sbbf.blockCheck(blk, someV)).to.eq(false)

        sbbf.blockInsert(blk, someY)
        expect(sbbf.blockCheck(blk, someY)).to.eq(true)

        makeListN(1000, () => {
            sbbf.blockInsert(blk, Number(generateHexString(31)))
        })

        expect(sbbf.blockCheck(blk, someV)).to.eq(false)
        expect(sbbf.blockCheck(blk, someY)).to.eq(true)
        expect(sbbf.blockCheck(blk, someX)).to.eq(true)
    })
    it("filter insert + check works", () => {
        const zees = [8, 32, 64, 128]
        const exes = [
            new Long(0xFFFFFFFF, 0x7FFFFFFF, true),
            new Long(0xABCDEF98, 0x70000000, true),
            new Long(0xDEADBEEF, 0x7FFFFFFF, true),
            new Long(0x0, 0x7FFFFFFF, true),
            new Long(0xC0FFEE3, 0x0, true),
            new Long(0x0, 0x1, true),
            new Long(793516929, -2061372197, true) // regression test; this one was failing get blockIndex
        ]

        zees.forEach((z) => {
            const filter = sbbf.initSplitBlocks(z)
            const badVal = Long.fromNumber(0xfafafafa, true)
            exes.forEach((x) => {
                sbbf.filterInsert(filter, x)
                expect(sbbf.filterCheck(filter, x)).to.eq(true)
                expect(sbbf.filterCheck(filter, badVal)).to.eq(false)
            })
        })
    })
})
