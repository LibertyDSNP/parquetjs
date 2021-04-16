import Long = require('long')
import {assert, expect} from "chai"
import * as sinon from "sinon"

import {generateHexString, makeListN} from "./util/general";
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
        for (let i = 0; i < expectedVals.length; i++) {
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
        const zees = [32, 128, 1024, 99]

        zees.forEach((z) => {
            const filter = new SplitBlockBloomFilter().setOptionNumFilterBytes(z).init()
            exes.forEach((x) => {
                filter.insert(x)
            })
            exes.forEach((x) => expect(filter.check(x)).to.eq(true))
            expect(filter.check(badVal)).to.eq(false)
        })
    })
    it("number of filter bytes is set to defaults on init", () => {
        const filter = new SplitBlockBloomFilter().init()
        exes.forEach((x) => {
            filter.insert(x)
        })
        exes.forEach((x) => expect(filter.check(x)).to.eq(true))
        expect(filter.check(badVal)).to.eq(false)
        expect(filter.optNumFilterBytes()).to.eq(16777216)
    })

    describe("setOptionNumBytes", () => {
        it("does not set invalid values", () => {
            const filter = new SplitBlockBloomFilter().init()
            const filterBytes = filter.optNumFilterBytes()
            const badZees = [-1, 512, 1023]

            badZees.forEach((bz) => {
                const spy = sinon.spy(console, "error")
                filter.setOptionNumFilterBytes(bz)
                expect(filter.optNumFilterBytes()).to.eq(filterBytes)
                expect(spy.calledOnce)
                spy.restore()
            })
        })
        it("sets filter bytes to next power of 2", () => {
            let filter = new SplitBlockBloomFilter().init()
            expect(filter.optNumFilterBytes()).to.eq(16777216)

            filter = new SplitBlockBloomFilter()
                .setOptionNumFilterBytes(1024)
                .init()
            expect(filter.optNumFilterBytes()).to.eq(1024)

            filter = new SplitBlockBloomFilter().setOptionNumFilterBytes(1025).init()
            expect(filter.optNumFilterBytes()).to.eq(2048)

            const below2 = 2**12 - 1
            filter = new SplitBlockBloomFilter().setOptionNumFilterBytes(below2).init()
            expect(filter.optNumFilterBytes()).to.eq(2**12)
        })
        it("can't be set twice after initializing", () => {
            const spy = sinon.spy(console, "error")
            const filter = new SplitBlockBloomFilter()
                .setOptionNumFilterBytes(333333)
                .setOptionNumFilterBytes(2**20)
                .init()
            expect(spy.notCalled)
            filter.setOptionNumFilterBytes(44444)
            expect(spy.calledOnce)
            expect(filter.optNumFilterBytes()).to.eq(2**20)
            spy.restore()
        })
    })

    describe("setOptionFalsePositiveRate", () => {
        it("can be set", () => {
            const filter = new SplitBlockBloomFilter().setOptionFalsePositiveRate(.001010)
            expect(filter.optFalsePositiveRate()).to.eq(.001010)
        })
        it("can't be set twice after initializing", () => {
            const spy = sinon.spy(console, "error")
            const filter = new SplitBlockBloomFilter()
                .setOptionFalsePositiveRate(.001010)
                .setOptionFalsePositiveRate(.002)
                .init()
            expect(spy.notCalled)
            filter.setOptionFalsePositiveRate(.0099)
            expect(spy.calledOnce)
            expect(filter.optFalsePositiveRate()).to.eq(.002)
            spy.restore()
        })
    })

    describe("setOptionNumDistinct", () => {
        it("can be set", () => {
            const filter = new SplitBlockBloomFilter().setOptionNumDistinct(10000)
            expect(filter.optNumDistinct()).to.eq(10000)
        })
        it("can't be set twice after initializing", () => {
            const spy = sinon.spy(console, "error")
            const filter = new SplitBlockBloomFilter()
                .setOptionNumDistinct(10000)
                .setOptionNumDistinct(9999)
            expect(spy.notCalled)
            filter.init().setOptionNumDistinct(38383)
            expect(filter.optNumDistinct()).to.eq(9999)
            expect(spy.calledOnce)
            spy.restore()
        })
    })

    describe("init", () => {
        it("does not allocate filter twice", () => {
            const spy = sinon.spy(console, "error")
            new SplitBlockBloomFilter().setOptionNumFilterBytes(1024).init().init()
            expect(spy.calledOnce)
            spy.restore()
        })
        it("allocates the filter", () => {
            const filter = new SplitBlockBloomFilter().setOptionNumFilterBytes(1024).init()
            expect(filter.numFilterBlocks()).to.eq(32)
            expect(filter.filter().length).to.eq(32)
        })
    })
    describe("optimal number of blocks", () => {
        it("sets correct values", () =>{
            let filter = new SplitBlockBloomFilter()
                .setOptionNumDistinct(32767)
                .setOptionFalsePositiveRate(.001)
                .init()
            expect(filter.numFilterBlocks()).to.eq(1825)
        })
    })
})
