import { assert, expect } from 'chai';
import {ParquetEnvelopeReader, ParquetReader} from "../parquet";
import {GetObjectCommand, HeadObjectCommand, S3Client} from '@aws-sdk/client-s3';
import {mockClient} from 'aws-sdk-client-mock';
import {sdkStreamMixin} from '@smithy/util-stream';
import {createReadStream} from 'fs';
import { Readable } from 'stream';
describe('ParquetReader with S3', () => {
  describe('V3', () => {
    const s3Mock = mockClient(S3Client);
    it('mocks get object', async () => {
      const stream = createReadStream('test/test-files/nation.plain.parquet');

      // wrap the Stream with SDK mixin
      const sdkStream = sdkStreamMixin(stream);

      s3Mock.on(GetObjectCommand).resolves({Body: sdkStream});
      s3Mock.on(HeadObjectCommand).resolves({ ContentLength: 1234 });

      const s3 = new S3Client({});
      const res: ParquetReader = await ParquetReader.openS3(s3, {Key: 'foo', Bucket: 'bar'});
      assert(res !== undefined);
    });
  })
})