"use strict";
const chai = require("chai");
const { expect } = chai;
const sinon = require("sinon");

const bloomFilterWriter = require("../lib/bloomFilterWriter.js");
const SplitBlockBloomFilter = require("../lib/bloom/sbbf").default;

describe("buildFilterBlocks", () => {
  describe("when no options are present", () => {
    let sbbfMock;
    beforeEach(() => {
      sbbfMock = sinon.mock(SplitBlockBloomFilter.prototype);
    });

    afterEach(() => {
      sbbfMock.verify();
    });

    it("calls .init once", () => {
      sbbfMock.expects("init").once();
      bloomFilterWriter.createSBBF({});
    });

    it("does not set false positive rate", () => {
      sbbfMock.expects("setOptionNumFilterBytes").never();
      bloomFilterWriter.createSBBF({});
    });

    it("does not set number of distinct", () => {
      sbbfMock.expects("setOptionNumDistinct").never();
      bloomFilterWriter.createSBBF({});
    });
  });

  describe("when numFilterBytes is present", () => {
    let sbbfMock;
    beforeEach(() => {
      sbbfMock = sinon.mock(SplitBlockBloomFilter.prototype);
    });

    afterEach(() => {
      sbbfMock.verify();
    });

    it("calls setOptionNumberFilterBytes once", () => {
      sbbfMock.expects("setOptionNumFilterBytes").once().returnsThis();
      bloomFilterWriter.createSBBF({ numFilterBytes: 1024 });
    });

    it("does not set number of distinct", () => {
      sbbfMock.expects("setOptionNumDistinct").never();
      bloomFilterWriter.createSBBF({});
    });

    it("calls .init once", () => {
      sbbfMock.expects("init").once();
      bloomFilterWriter.createSBBF({});
    });
  });

  describe("when numFilterBytes is NOT present", () => {
    let sbbfMock;
    beforeEach(() => {
      sbbfMock = sinon.mock(SplitBlockBloomFilter.prototype);
    });

    afterEach(() => {
      sbbfMock.verify();
    });

    describe("and falsePositiveRate is present", () => {
      it("calls ssbf.setOptionFalsePositiveRate", () => {
        sbbfMock.expects("setOptionFalsePositiveRate").once();
        bloomFilterWriter.createSBBF({ falsePositiveRate: 0.1 });
      });
    });

    describe("and numDistinct is present", () => {
      it("calls ssbf.setOptionNumDistinct", () => {
        sbbfMock.expects("setOptionNumDistinct").once();
        bloomFilterWriter.createSBBF({
          falsePositiveRate: 0.1,
          numDistinct: 1,
        });
      });
    });
  });
});
