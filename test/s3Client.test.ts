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
      const headStream = new Readable();
      headStream.push('PAR1');
      headStream.push(null);
      const headSdkStream = sdkStreamMixin(headStream)

      const footStream = new Readable();
      footStream.push(Uint8Array.from([234,0,0,0])); // metadata length is 234
      footStream.push('PAR1');
      footStream.push(null);
      const footSdkStream = sdkStreamMixin(footStream)

      const stream = createReadStream('test/test-files/nation.dict.parquet');

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


      const s3 = new S3Client({});
      try {
        await ParquetReader.openS3(s3, {Key: 'foo', Bucket: 'bar'});
      } catch (e: any) {
        assert(e.toString().includes('invalid parquet version'))
      }
    });
  })
})