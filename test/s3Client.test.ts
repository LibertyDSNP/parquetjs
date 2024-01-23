import {assert, expect} from 'chai';
import {ParquetEnvelopeReader, ParquetReader} from "../parquet";
import {GetObjectCommand, HeadObjectCommand, S3Client} from '@aws-sdk/client-s3';
import {mockClient} from 'aws-sdk-client-mock';
import {sdkStreamMixin} from '@smithy/util-stream';
import {createReadStream} from 'fs';
import {Readable} from 'stream';

describe('ParquetReader with S3', () => {
  describe('V3', () => {
    const s3Mock = mockClient(S3Client);

    it('mocks get object', async () => {
      let srcFile = 'test/test-files/nation.dict.parquet';

      const headStream = new Readable();
      headStream.push('PAR1');
      headStream.push(null);
      const headSdkStream = sdkStreamMixin(headStream)

      const footStream = createReadStream(srcFile, {start: 2842, end: 2849})
      const footSdkStream= sdkStreamMixin(footStream);

      const metadataStream = createReadStream(srcFile, {start: 2608, end: 2841});
      const metaDataSdkStream = sdkStreamMixin(metadataStream)

      const stream = createReadStream(srcFile);

      // wrap the Stream with SDK mixin
      const sdkStream = sdkStreamMixin(stream);

      // mock all the way down to where metadata is being read
      s3Mock.on(HeadObjectCommand)
            .resolves({ContentLength: 2849});

      s3Mock.on(GetObjectCommand,)
            .resolves({Body: sdkStream});

      s3Mock.on(GetObjectCommand, {Range: 'bytes=0-3', Key: 'foo', Bucket: 'bar'})
            .resolves({Body: headSdkStream});

      s3Mock.on(GetObjectCommand, {Range: 'bytes=2841-2848', Key: 'foo', Bucket: 'bar'})
            .resolves({Body: footSdkStream});

      s3Mock.on(GetObjectCommand, {Range: 'bytes=2607-2840', Key: 'foo', Bucket: 'bar'})
      .resolves({Body: metaDataSdkStream});

      const s3 = new S3Client({});
      let res = await ParquetReader.openS3(s3, {Key: 'foo', Bucket: 'bar'});
      assert(res.envelopeReader);
    });
  })
})