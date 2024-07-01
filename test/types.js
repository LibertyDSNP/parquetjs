'use strict';
const { toPrimitive } = require('../lib/types');
const chai = require('chai');
const assert = chai.assert;

describe('toPrimitive* should give the correct values back', function () {
  it('toPrimitive(INT_8, 127n)', function () {
    assert.equal(toPrimitive('INT_8', 127n), 127n);
  }),
    it('toPrimitive(UINT_8, 255n)', function () {
      assert.equal(toPrimitive('UINT_8', 255n), 255n);
    }),
    it('toPrimitive(INT_16, 32767n)', function () {
      assert.equal(toPrimitive('INT_16', 32767n), 32767n);
    }),
    it('toPrimitive(UINT_16, 65535n)', function () {
      assert.equal(toPrimitive('UINT_16', 65535n), 65535n);
    }),
    it('toPrimitive(INT32, 2147483647n)', function () {
      assert.equal(toPrimitive('INT32', 2147483647n), 2147483647n);
    }),
    it('toPrimitive(UINT_32, 4294967295n)', function () {
      assert.equal(toPrimitive('UINT_32', 4294967295n), 4294967295n);
    }),
    it('toPrimitive(INT64, 9223372036854775807n)', function () {
      assert.equal(toPrimitive('INT64', 9223372036854775807n), 9223372036854775807n);
    }),
    it('toPrimitive(UINT_64, 9223372036854775807n)', function () {
      assert.equal(toPrimitive('UINT_64', 9223372036854775807n), 9223372036854775807n);
    }),
    it('toPrimitive(INT96, 9223372036854775807n)', function () {
      assert.equal(toPrimitive('INT96', 9223372036854775807n), 9223372036854775807n);
    });
});

describe('toPrimitive INT* should give the correct values back with string value', function () {
  it('toPrimitive(INT_8, "127")', function () {
    assert.equal(toPrimitive('INT_8', '127'), 127n);
  }),
    it('toPrimitive(UINT_8, "255")', function () {
      assert.equal(toPrimitive('UINT_8', '255'), 255n);
    }),
    it('toPrimitive(INT_16, "32767")', function () {
      assert.equal(toPrimitive('INT_16', '32767'), 32767n);
    }),
    it('toPrimitive(UINT_16, "65535")', function () {
      assert.equal(toPrimitive('UINT_16', '65535'), 65535n);
    }),
    it('toPrimitive(INT32, "2147483647")', function () {
      assert.equal(toPrimitive('INT32', '2147483647'), 2147483647n);
    }),
    it('toPrimitive(UINT_32, "4294967295")', function () {
      assert.equal(toPrimitive('UINT_32', '4294967295'), 4294967295n);
    }),
    it('toPrimitive(INT64, "9223372036854775807")', function () {
      assert.equal(toPrimitive('INT64', '9223372036854775807'), 9223372036854775807n);
    }),
    it('toPrimitive(UINT_64, "9223372036854775807")', function () {
      assert.equal(toPrimitive('UINT_64', '9223372036854775807'), 9223372036854775807n);
    }),
    it('toPrimitive(INT96, "9223372036854775807")', function () {
      assert.equal(toPrimitive('INT96', '9223372036854775807'), 9223372036854775807n);
    });
});

describe('toPrimitive INT* should throw when given invalid value', function () {
  describe('Testing toPrimitive_INT_8 values', function () {
    it('toPrimitive(INT_8, 128) is too large', function () {
      assert.throws(() => toPrimitive('INT_8', 128));
    }),
      it('toPrimitive(INT_8, -256) is too small', function () {
        assert.throws(() => toPrimitive('INT_8', -256));
      }),
      it('toPrimitive(INT_8, "asd12@!$1") is given gibberish and should throw', function () {
        assert.throws(() => toPrimitive('INT_8', 'asd12@!$1'));
      });
  }),
    describe('Testing toPrimitive_UINT8 values', function () {
      it('toPrimitive(UINT_8, 128) is too large', function () {
        assert.throws(() => toPrimitive('UINT_8', 256));
      }),
        it('toPrimitive(UINT_8, -256) is too small', function () {
          assert.throws(() => toPrimitive('UINT_8', -1));
        }),
        it('toPrimitive(UINT_8, "asd12@!$1") is given gibberish and should throw', function () {
          assert.throws(() => toPrimitive('UINT_8', 'asd12@!$1'));
        });
    }),
    describe('Testing toPrimitive_INT16 values', function () {
      it('toPrimitive(INT_16, 9999999) is too large', function () {
        assert.throws(() => toPrimitive('INT_16', 9999999));
      }),
        it('toPrimitive(INT_16, -9999999) is too small', function () {
          assert.throws(() => toPrimitive('INT_16', -9999999));
        }),
        it('toPrimitive(INT_16, "asd12@!$1") is given gibberish and should throw', function () {
          assert.throws(() => toPrimitive('INT_16', 'asd12@!$1'));
        });
    }),
    describe('Testing toPrimitive_UINT16 values', function () {
      it('toPrimitive(UINT_16, 9999999999999) is too large', function () {
        assert.throws(() => toPrimitive('UINT_16', 9999999999999));
      }),
        it('toPrimitive(UINT_16, -999999999999) is too small', function () {
          assert.throws(() => toPrimitive('UINT_16', -9999999999999));
        }),
        it('toPrimitive(UINT_16, "asd12@!$1") is given gibberish and should throw', function () {
          assert.throws(() => toPrimitive('UINT_16', 'asd12@!$1'));
        });
    }),
    describe('Testing toPrimitive_INT32 values', function () {
      it('toPrimitive(INT_32, 999999999999) is too large', function () {
        assert.throws(() => toPrimitive('INT_32', 999999999999));
      }),
        it('toPrimitive(INT_32, -999999999999) is too small', function () {
          assert.throws(() => toPrimitive('INT_32', -999999999999));
        }),
        it('toPrimitive(INT_32, "asd12@!$1") is given gibberish and should throw', function () {
          assert.throws(() => toPrimitive('INT_32', 'asd12@!$1'));
        });
    }),
    describe('Testing toPrimitive_UINT32 values', function () {
      it('toPrimitive(UINT_32, 999999999999) is too large', function () {
        assert.throws(() => toPrimitive('UINT_32', 999999999999999));
      }),
        it('toPrimitive(UINT_32, -999999999999) is too small', function () {
          assert.throws(() => toPrimitive('UINT_32', -999999999999));
        }),
        it('toPrimitive(UINT_32, "asd12@!$1") is given gibberish and should throw', function () {
          assert.throws(() => toPrimitive('UINT_32', 'asd12@!$1'));
        });
    }),
    describe('Testing toPrimitive_INT64 values', function () {
      it('toPrimitive(INT_64, "9999999999999999999999") is too large', function () {
        assert.throws(() => toPrimitive('INT_64', 9999999999999999999999));
      }),
        it('toPrimitive(INT_64, "-9999999999999999999999999") is too small', function () {
          assert.throws(() => toPrimitive('INT_64', -9999999999999999999999999));
        }),
        it('toPrimitive(INT_64, "asd12@!$1") is given gibberish and should throw', function () {
          assert.throws(() => toPrimitive('INT_64', 'asd12@!$1'));
        });
    }),
    describe('Testing toPrimitive_UINT64 values', function () {
      it('toPrimitive(UINT_64, 9999999999999999999999) is too large', function () {
        assert.throws(() => toPrimitive('UINT_64', 9999999999999999999999));
      }),
        it('toPrimitive(UINT_64, -999999999999) is too small', function () {
          assert.throws(() => toPrimitive('UINT_64', -999999999999));
        }),
        it('toPrimitive(UINT_64, "asd12@!$1") is given gibberish and should throw', function () {
          assert.throws(() => toPrimitive('UINT_64', 'asd12@!$1'));
        });
    }),
    describe('Testing toPrimitive_INT96 values', function () {
      it('toPrimitive(UINT_96, 9999999999999999999999) is too large', function () {
        assert.throws(() => toPrimitive('INT_96', 9999999999999999999999));
      }),
        it('toPrimitive(UINT_96, -9999999999999999999999) is too small', function () {
          assert.throws(() => toPrimitive('INT_96', -9999999999999999999999));
        }),
        it('toPrimitive(UINT_96, "asd12@!$1") is given gibberish and should throw', function () {
          assert.throws(() => toPrimitive('INT_96', 'asd12@!$1'));
        });
    });

  describe('toPrimitive ', function () {
    // eslint-disable-next-line mocha/no-setup-in-describe
    ['TIME_MILLIS', 'TIME_MICROS', 'DATE', 'TIMESTAMP_MILLIS', 'TIMESTAMP_MICROS'].forEach((typeName) => {
      it(`for type ${typeName} happy path`, function () {
        assert.equal(1234, toPrimitive(typeName, 1234));
        assert.equal(1234, toPrimitive(typeName, '1234'));
      });
      it(`for type ${typeName} fails with negative values`, function () {
        assert.throws(() => toPrimitive(typeName, '-1'), `${typeName} value is out of bounds: -1`);
        assert.throws(() => toPrimitive(typeName, -1), `${typeName} value is out of bounds: -1`);
      });
    });
    // eslint-disable-next-line mocha/no-setup-in-describe
    ['DATE', 'TIMESTAMP_MILLIS', 'TIME_MILLIS'].forEach((typeName) => {
      it(`${typeName} throws when number too large`, function () {
        assert.throws(() => toPrimitive(typeName, 9999999999999999999999), `${typeName} value is out of bounds: 1e+22`);
        assert.throws(
          () => toPrimitive(typeName, '9999999999999999999999'),
          `${typeName} value is out of bounds: 1e+22`
        );
      });
    });

    it('DATE conversion works for DATE type', function () {
      const date = new Date(Date.parse('2022-12-01:00:00:01 GMT'));
      assert.equal(toPrimitive('DATE', date), 19327.000011574073);
    });

    it('TIMESTAMP_MICROS works for a Date type and bigint', function () {
      const date = new Date(Date.parse('2022-12-01:00:00:01 GMT'));
      assert.equal(toPrimitive('TIMESTAMP_MICROS', date), 1669852801000000n);
      assert.equal(toPrimitive('TIMESTAMP_MICROS', '9999999999999999999999'), 9999999999999999999999n);
      assert.equal(toPrimitive('TIMESTAMP_MICROS', 98989898n), 98989898n);
    });

    it('TIME_MICROS works for a bigint', function () {
      const timestampBigint = 1932733334490741n;
      assert.equal(toPrimitive('TIME_MICROS', timestampBigint), 1932733334490741);
      assert.equal(toPrimitive('TIME_MICROS', '9999999999999999999999'), 9999999999999999999999n);
      assert.equal(toPrimitive('TIME_MICROS', 9999999999999999999999n), 9999999999999999999999n);
    });
  });
});
