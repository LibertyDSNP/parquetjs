import sinon from 'sinon';

import { createSBBF } from '../lib/bloomFilterIO/bloomFilterWriter';
const SplitBlockBloomFilter = require('../lib/bloom/sbbf').default;

describe('buildFilterBlocks', () => {
  describe('when no options are present', () => {
    let sbbfMock: sinon.SinonMock;

    beforeEach(() => {
      sbbfMock = sinon.mock(SplitBlockBloomFilter.prototype);
    });

    afterEach(() => {
      sbbfMock.verify();
    });

    it('calls .init once', () => {
      sbbfMock.expects('init').once();
      createSBBF({});
    });

    it('does not set false positive rate', () => {
      sbbfMock.expects('setOptionNumFilterBytes').never();
      createSBBF({});
    });

    it('does not set number of distinct', () => {
      sbbfMock.expects('setOptionNumDistinct').never();
      createSBBF({});
    });
  });

  describe('when numFilterBytes is present', () => {
    let sbbfMock: sinon.SinonMock;

    beforeEach(() => {
      sbbfMock = sinon.mock(SplitBlockBloomFilter.prototype);
    });

    afterEach(() => {
      sbbfMock.verify();
    });

    it('calls setOptionNumberFilterBytes once', () => {
      sbbfMock.expects('setOptionNumFilterBytes').once().returnsThis();
      createSBBF({ numFilterBytes: 1024 });
    });

    it('does not set number of distinct', () => {
      sbbfMock.expects('setOptionNumDistinct').never();
      createSBBF({});
    });

    it('calls .init once', () => {
      sbbfMock.expects('init').once();
      createSBBF({});
    });
  });

  describe('when numFilterBytes is NOT present', () => {
    let sbbfMock: sinon.SinonMock;
    beforeEach(() => {
      sbbfMock = sinon.mock(SplitBlockBloomFilter.prototype);
    });

    afterEach(() => {
      sbbfMock.verify();
    });

    describe('and falsePositiveRate is present', () => {
      it('calls ssbf.setOptionFalsePositiveRate', () => {
        sbbfMock.expects('setOptionFalsePositiveRate').once();
        createSBBF({ falsePositiveRate: 0.1 });
      });
    });

    describe('and numDistinct is present', () => {
      it('calls ssbf.setOptionNumDistinct', () => {
        sbbfMock.expects('setOptionNumDistinct').once();
        createSBBF({
          falsePositiveRate: 0.1,
          numDistinct: 1,
        });
      });
    });
  });
});
