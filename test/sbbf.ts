import Long = require('long')
import {assert, expect} from "chai"
import * as sinon from "sinon"

import {generateHexString, makeListN, randInt, times} from "./util/general";
import SplitBlockBloomFilter from "../lib/bloom/sbbf";
const XxHash = require("xxhash")
describe("Split Block Bloom Filters", () => {
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

            const below2 = 2 ** 12 - 1
            filter = new SplitBlockBloomFilter().setOptionNumFilterBytes(below2).init()
            expect(filter.optNumFilterBytes()).to.eq(2 ** 12)
        })
        it("can't be set twice after initializing", () => {
            const spy = sinon.spy(console, "error")
            const filter = new SplitBlockBloomFilter()
                .setOptionNumFilterBytes(333333)
                .setOptionNumFilterBytes(2 ** 20)
                .init()
            expect(spy.notCalled)
            filter.setOptionNumFilterBytes(44444)
            expect(spy.calledOnce)
            expect(filter.optNumFilterBytes()).to.eq(2 ** 20)
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
        // Some general ideas of what size filters are needed for different parameters
        it("can be called", () => {
            expect(SplitBlockBloomFilter.optimalNumOfBlocks(20000, 0.001)).to.eq(1143)
            expect(SplitBlockBloomFilter.optimalNumOfBlocks(20000, 0.0001)).to.eq(1645)
            expect(SplitBlockBloomFilter.optimalNumOfBlocks(50000, 0.0001)).to.eq(4111)
            expect(SplitBlockBloomFilter.optimalNumOfBlocks(50000, 0.00001)).to.eq(5773)
            expect(SplitBlockBloomFilter.optimalNumOfBlocks(100000, 0.000001)).to.eq(15961)
        })

        it("sets good values", (done) => {
            const numDistinct = 100000
            const fpr = 0.01
            const filter = new SplitBlockBloomFilter()
                .setOptionNumDistinct(numDistinct)
                .setOptionFalsePositiveRate(fpr)
                .init()

            times(numDistinct, () => {
                const hashValue = new Long(randInt(2 ** 30), randInt(2 ** 30), true)
                filter.insert(hashValue)
                expect(filter.check(hashValue))
            })

            let falsePositive = 0
            times(numDistinct, () => {
                const notInFilter = new Long(randInt(2 ** 30), randInt(2 ** 30), true)
                if (!filter.check(notInFilter)) falsePositive++
            })

            if (falsePositive > 0) console.log(falsePositive)
            expect(falsePositive < (numDistinct * fpr))
            done()
        }).timeout(5000)
    })
})
