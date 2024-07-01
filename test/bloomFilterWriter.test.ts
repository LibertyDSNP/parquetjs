import sinon from 'sinon';

import { createSBBF } from '../lib/bloomFilterIO/bloomFilterWriter';
const SplitBlockBloomFilter = require('../lib/bloom/sbbf').default;

describe('buildFilterBlocks', function () {
  describe('when no options are present', function () {
    let sbbfMock: sinon.SinonMock;

    beforeEach(function () {
      sbbfMock = sinon.mock(SplitBlockBloomFilter.prototype);
    });

    afterEach(function () {
      sbbfMock.verify();
    });

    it('calls .init once', function () {
      sbbfMock.expects('init').once();
      createSBBF({});
    });

    it('does not set false positive rate', function () {
      sbbfMock.expects('setOptionNumFilterBytes').never();
      createSBBF({});
    });

    it('does not set number of distinct', function () {
      sbbfMock.expects('setOptionNumDistinct').never();
      createSBBF({});
    });
  });

  describe('when numFilterBytes is present', function () {
    let sbbfMock: sinon.SinonMock;

    beforeEach(function () {
      sbbfMock = sinon.mock(SplitBlockBloomFilter.prototype);
    });

    afterEach(function () {
      sbbfMock.verify();
    });

    it('calls setOptionNumberFilterBytes once', function () {
      sbbfMock.expects('setOptionNumFilterBytes').once().returnsThis();
      createSBBF({ numFilterBytes: 1024 });
    });

    it('does not set number of distinct', function () {
      sbbfMock.expects('setOptionNumDistinct').never();
      createSBBF({});
    });

    it('calls .init once', function () {
      sbbfMock.expects('init').once();
      createSBBF({});
    });
  });

  describe('when numFilterBytes is NOT present', function () {
    let sbbfMock: sinon.SinonMock;

    beforeEach(function () {
      sbbfMock = sinon.mock(SplitBlockBloomFilter.prototype);
    });

    afterEach(function () {
      sbbfMock.verify();
    });

    describe('and falsePositiveRate is present', function () {
      it('calls ssbf.setOptionFalsePositiveRate', function () {
        sbbfMock.expects('setOptionFalsePositiveRate').once();
        createSBBF({ falsePositiveRate: 0.1 });
      });
    });

    describe('and numDistinct is present', function () {
      it('calls ssbf.setOptionNumDistinct', function () {
        sbbfMock.expects('setOptionNumDistinct').once();
        createSBBF({
          falsePositiveRate: 0.1,
          numDistinct: 1,
        });
      });
    });
  });
});
