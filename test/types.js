'use strict';
const  { toPrimitive, fromPrimitive } = require("../lib/types.js") 
const chai = require('chai');
const assert = chai.assert;

describe("toPrimitive INT* should give the correct values back", () => {
    it('toPrimitive(INT_8, 127)', () => {
        assert.equal(toPrimitive('INT_8',127n), 127n)
    }),
    it('toPrimitive(UINT_8, 255n)', () => {
        assert.equal(toPrimitive('UINT_8',255n), 255n)
    }),
    it('toPrimitive(INT_16, 32767n)', () => {
        assert.equal(toPrimitive('INT_16',32767n), 32767n)
    }),
    it('toPrimitive(UINT_16, 65535n)', () => {
        assert.equal(toPrimitive('UINT_16',65535n), 65535n)
    }),
    it('toPrimitive(INT32, 2147483647n)', () => {
        assert.equal(toPrimitive('INT32',2147483647n), 2147483647n)
    }),
    it('toPrimitive(UINT_32, 4294967295n)', () => {
        assert.equal(toPrimitive('UINT_32',4294967295n), 4294967295n)
    }),
    it('toPrimitive(INT64, 9223372036854775807n)', () => {
        assert.equal(toPrimitive('INT64',9223372036854775807n), 9223372036854775807n)
    }),
    it('toPrimitive(UINT_64, 9223372036854775807n)', () => {
        assert.equal(toPrimitive('UINT_64',9223372036854775807n), 9223372036854775807n)
    }),
    it('toPrimitive(INT96, 9223372036854775807n)', () => {
        assert.equal(toPrimitive('INT96',9223372036854775807n), 9223372036854775807n)
    })
})

describe("toPrimitive INT* should give the correct values back with string value", () => {
    it('toPrimitive(INT_8, "127")', () => {
        assert.equal(toPrimitive('INT_8',"127"), 127n)
    }),
    it('toPrimitive(UINT_8, "255")', () => {
        assert.equal(toPrimitive('UINT_8',"255"), 255n)
    }),
    it('toPrimitive(INT_16, "32767")', () => {
        assert.equal(toPrimitive('INT_16',"32767"), 32767n)
    }),
    it('toPrimitive(UINT_16, "65535")', () => {
        assert.equal(toPrimitive('UINT_16',"65535"), 65535n)
    }),
    it('toPrimitive(INT32, "2147483647")', () => {
        assert.equal(toPrimitive('INT32',"2147483647"), 2147483647n)
    }),
    it('toPrimitive(UINT_32, "4294967295")', () => {
        assert.equal(toPrimitive('UINT_32',"4294967295"), 4294967295n)
    }),
    it('toPrimitive(INT64, "9223372036854775807")', () => {
        assert.equal(toPrimitive('INT64',"9223372036854775807"), 9223372036854775807n)
    }),
    it('toPrimitive(UINT_64, "9223372036854775807")', () => {
        assert.equal(toPrimitive('UINT_64',"9223372036854775807"), 9223372036854775807n)
    }),
    it('toPrimitive(INT96, "9223372036854775807")', () => {
        assert.equal(toPrimitive('INT96',"9223372036854775807"), 9223372036854775807n)
    })
})
