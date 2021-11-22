export {}
'use strict';
const  { toPrimitive, fromPrimitive } = require("../lib/types.ts") 
const chai = require('chai');
const assert = chai.assert;

describe("toPrimitive INT* should give the correct values back", () => {
    it('toPrimitive(INT_8, 127n)', () => {
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


describe("toPrimitive INT* should throw when given invalid value", () => {
    describe("Testing toPrimitive_INT_8 values", () => {
        it('toPrimitive(INT_8, 128) is too large', () => {
            assert.throws(() => toPrimitive('INT_8',128))
        }),
        it('toPrimitive(INT_8, -256) is too small', () => {
            assert.throws(() => toPrimitive('INT_8',-256))
        }),
        it('toPrimitive(INT_8, "asd12@!$1") is given gibberish and should throw', () => {
            assert.throws(() => toPrimitive('INT_8', "asd12@!$1"))
        })
    }),
    describe("Testing toPrimitive_UINT8 values", () => {
        it('toPrimitive(UINT_8, 128) is too large', () => {
            assert.throws(() => toPrimitive('UINT_8',256))
        }),
        it('toPrimitive(UINT_8, -256) is too small', () => {
            assert.throws(() => toPrimitive('UINT_8',-1))
        }),
        it('toPrimitive(UINT_8, "asd12@!$1") is given gibberish and should throw', () => {
            assert.throws(() => toPrimitive('UINT_8', "asd12@!$1"))
        })
    }),
    describe("Testing toPrimitive_INT16 values", () => {
        it('toPrimitive(INT_16, 9999999) is too large', () => {
            assert.throws(() => toPrimitive('INT_16',9999999))
        }),
        it('toPrimitive(INT_16, -9999999) is too small', () => {
            assert.throws(() => toPrimitive('INT_16',-9999999))
        }),
        it('toPrimitive(INT_16, "asd12@!$1") is given gibberish and should throw', () => {
            assert.throws(() => toPrimitive('INT_16', "asd12@!$1"))
        })
    }),
    describe("Testing toPrimitive_UINT16 values", () => {
        it('toPrimitive(UINT_16, 9999999999999) is too large', () => {
            assert.throws(() => toPrimitive('UINT_16',9999999999999))
        }),
        it('toPrimitive(UINT_16, -999999999999) is too small', () => {
            assert.throws(() => toPrimitive('UINT_16',-9999999999999))
        }),
        it('toPrimitive(UINT_16, "asd12@!$1") is given gibberish and should throw', () => {
            assert.throws(() => toPrimitive('UINT_16', "asd12@!$1"))
        })
    }),
    describe("Testing toPrimitive_INT32 values", () => {
        it('toPrimitive(INT_32, 999999999999) is too large', () => {
            assert.throws(() => toPrimitive('INT_32',999999999999))
        }),
        it('toPrimitive(INT_32, -999999999999) is too small', () => {
            assert.throws(() => toPrimitive('INT_32',-999999999999))
        }),
        it('toPrimitive(INT_32, "asd12@!$1") is given gibberish and should throw', () => {
            assert.throws(() => toPrimitive('INT_32', "asd12@!$1"))
        })
    }),
    describe("Testing toPrimitive_UINT32 values", () => {
        it('toPrimitive(UINT_32, 999999999999) is too large', () => {
            assert.throws(() => toPrimitive('UINT_32',999999999999999))
        }),
        it('toPrimitive(UINT_32, -999999999999) is too small', () => {
            assert.throws(() => toPrimitive('UINT_32',-999999999999))
        }),
        it('toPrimitive(UINT_32, "asd12@!$1") is given gibberish and should throw', () => {
            assert.throws(() => toPrimitive('UINT_32', "asd12@!$1"))
        })
    }),
    describe("Testing toPrimitive_INT64 values", () => {
        it('toPrimitive(INT_64, "9999999999999999999999") is too large', () => {
            assert.throws(() => toPrimitive('INT_64', 9999999999999999999999))
        }),
        it('toPrimitive(INT_64, "-9999999999999999999999999") is too small', () => {
            assert.throws(() => toPrimitive('INT_64', -9999999999999999999999999))
        }),
        it('toPrimitive(INT_64, "asd12@!$1") is given gibberish and should throw', () => {
            assert.throws(() => toPrimitive('INT_64', "asd12@!$1"))
        })
    }),
    describe("Testing toPrimitive_UINT64 values", () => {
        it('toPrimitive(UINT_64, 9999999999999999999999) is too large', () => {
            assert.throws(() => toPrimitive('UINT_64',9999999999999999999999))
        }),
        it('toPrimitive(UINT_64, -999999999999) is too small', () => {
            assert.throws(() => toPrimitive('UINT_64',-999999999999))
        }),
        it('toPrimitive(UINT_64, "asd12@!$1") is given gibberish and should throw', () => {
            assert.throws(() => toPrimitive('UINT_64', "asd12@!$1"))
        })
    }),
    describe("Testing toPrimitive_INT96 values", () => {
        it('toPrimitive(UINT_96, 9999999999999999999999) is too large', () => {
            assert.throws(() => toPrimitive('INT_96',9999999999999999999999))
        }),
        it('toPrimitive(UINT_96, -9999999999999999999999) is too small', () => {
            assert.throws(() => toPrimitive('INT_96',-9999999999999999999999))
        }),
        it('toPrimitive(UINT_96, "asd12@!$1") is given gibberish and should throw', () => {
            assert.throws(() => toPrimitive('INT_96', "asd12@!$1"))
        })
    })
})
