import chai, {expect} from "chai"
import sinon from "sinon"
import sinonChai from "sinon-chai";
import sinonChaiInOrder from 'sinon-chai-in-order';
import BufferReader from "../../lib/bufferReader"

chai.use(sinonChai);
chai.use(sinonChaiInOrder);

describe.only("bufferReader", () => {
  let reader;

  beforeEach(() => {
    const mockEnvelopeReader = sinon.fake();
    reader = new BufferReader(mockEnvelopeReader, {});
  })
  describe("#read", async () => {
    describe("given that reader is scheduled", () => {
      it("adds an item to the queue", () => {
        const offset = 1;
        const length = 2;
        reader.read(offset, length);
        expect(reader.queue.length).to.eql(1);
      })
    })
  })

  describe("#processQueue", () => {
    it("only enqueues an item and reads on flushing the queue", async () => {
      const mockResolve = sinon.spy();
      const mockResolve2 = sinon.spy();
      reader.envelopeReader = {readFn: sinon.fake.returns("buffer")}

      reader.queue = [{
        offset: 1,
        length: 1,
        resolve: mockResolve,
      }, {
        offset: 2,
        length: 4,
        resolve: mockResolve2,
      }];

      await reader.processQueue();

      sinon.assert.calledWith(mockResolve, "b")
      sinon.assert.calledWith(mockResolve2, "uffe")
    })

    it("enqueues items and then reads them", async () => {
      const mockResolve = sinon.spy();
      const mockResolve2 = sinon.spy();
      reader.maxLength = 1;
      reader.envelopeReader = {readFn: sinon.fake.returns("buffer")}

      reader.queue = [{
        offset: 1,
        length: 1,
        resolve: mockResolve,
      }, {
        offset: 2,
        length: 4,
        resolve: mockResolve2,
      }];

      await reader.processQueue();

      sinon.assert.calledWith(mockResolve, "b")
      sinon.assert.calledWith(mockResolve2, "uffe")
    })

    it("enqueues items and reads them in order", async () => {
      const mockResolve = sinon.spy();
      reader.envelopeReader = {readFn: sinon.fake.returns("thisisalargebuffer")}

      reader.queue = [{
          offset: 1,
          length: 4,
          resolve: mockResolve,
        }, {
          offset: 5,
          length: 2,
          resolve: mockResolve,
        }, {
          offset: 7,
          length: 1,
          resolve: mockResolve,
        }, {
          offset: 8,
          length: 5,
          resolve: mockResolve,
        }, {
          offset: 13,
          length: 6,
          resolve: mockResolve,
        }
      ];

      await reader.processQueue();

      expect(mockResolve).inOrder.to.have.been.calledWith("this")
        .subsequently.calledWith("is")
        .subsequently.calledWith("a")
        .subsequently.calledWith("large")
        .subsequently.calledWith("buffer");
    })

    it.only("should read even if the maxSpan has been exceeded", async () => {
      const mockResolve = sinon.spy();
      reader.maxSpan = 5;
      reader.envelopeReader = {readFn: sinon.fake.returns("willslicefrombeginning")}

      reader.queue = [{
          offset: 1,
          length: 4,
          resolve: mockResolve,
        }, {
          offset: 10,
          length: 4,
          resolve: mockResolve,
        }, {
          offset: 10,
          length: 9,
          resolve: mockResolve,
        }, {
          offset: 10,
          length: 13,
          resolve: mockResolve,
        }, {
          offset: 10,
          length: 22,
          resolve: mockResolve,
        }
      ];

      await reader.processQueue();

      expect(mockResolve).inOrder.to.have.been.calledWith("will")
        .subsequently.calledWith("will")
        .subsequently.calledWith("willslice")
        .subsequently.calledWith("willslicefrom")
        .subsequently.calledWith("willslicefrombeginning");
    })
  })
})
