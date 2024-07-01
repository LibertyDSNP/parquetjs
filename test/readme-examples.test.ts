import { expect } from 'chai';

import { ParquetSchema } from '../parquet';

describe('Readme Encoding Examples', function () {
  it('PLAIN should work', function () {
    const ps = new ParquetSchema({
      name: { type: 'UTF8', encoding: 'PLAIN' },
    });
    expect(ps).to.be.a('object');
    expect(ps.schema.name.encoding).to.eq('PLAIN');
  });

  it('RLE should work', function () {
    const ps = new ParquetSchema({
      age: { type: 'UINT_32', encoding: 'RLE', typeLength: 7 },
    });
    expect(ps).to.be.a('object');
    expect(ps.schema.age.typeLength).to.eq(7);
  });
});
