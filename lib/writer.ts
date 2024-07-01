import stream from 'stream';
import parquet_thrift, { ConvertedType } from '../gen-nodejs/parquet_types';
import * as parquet_shredder from './shred';
import * as parquet_util from './util';
import * as parquet_codec from './codec';
import * as parquet_compression from './compression';
import * as parquet_types from './types';
import * as bloomFilterWriter from './bloomFilterIO/bloomFilterWriter';
import { WriterOptions, ParquetCodec, ParquetField, ColumnMetaDataExt, RowGroupExt, Page } from './declare';
import { Options } from './codec/types';
import { ParquetSchema } from './schema';
import Int64 from 'node-int64';
import SplitBlockBloomFilter from './bloom/sbbf';

/**
 * Parquet File Magic String
 */
const PARQUET_MAGIC = 'PAR1';

/**
 * Parquet File Format Version
 */
const PARQUET_VERSION = 1;

/**
 * Default Page and Row Group sizes
 */
const PARQUET_DEFAULT_PAGE_SIZE = 8192;
const PARQUET_DEFAULT_ROW_GROUP_SIZE = 4096;

/**
 * Repetition and Definition Level Encoding
 */
const PARQUET_RDLVL_TYPE = 'INT32';
const PARQUET_RDLVL_ENCODING = 'RLE';

/**
 * Write a parquet file to an output stream. The ParquetWriter will perform
 * buffering/batching for performance, so close() must be called after all rows
 * are written.
 */
export class ParquetWriter {
  schema: ParquetSchema;
  envelopeWriter: ParquetEnvelopeWriter | null;
  rowBuffer: parquet_shredder.RecordBuffer;
  rowGroupSize: number;
  closed: boolean;
  userMetadata: Record<string, string>;

  /**
   * Convenience method to create a new buffered parquet writer that writes to
   * the specified file
   */
  static async openFile(schema: ParquetSchema, path: string | Buffer | URL, opts?: WriterOptions) {
    const outputStream = await parquet_util.osopen(path, opts);
    return ParquetWriter.openStream(schema, outputStream, opts);
  }

  /**
   * Convenience method to create a new buffered parquet writer that writes to
   * the specified stream
   */
  static async openStream(schema: ParquetSchema, outputStream: parquet_util.WriteStreamMinimal, opts?: WriterOptions) {
    if (!opts) {
      opts = {};
    }

    const envelopeWriter = await ParquetEnvelopeWriter.openStream(schema, outputStream, opts);

    return new ParquetWriter(schema, envelopeWriter, opts);
  }

  /**
   * Create a new buffered parquet writer for a given envelope writer
   */
  constructor(schema: ParquetSchema, envelopeWriter: ParquetEnvelopeWriter, opts?: WriterOptions) {
    this.schema = schema;
    this.envelopeWriter = envelopeWriter;
    this.rowBuffer = {};
    this.rowGroupSize = (opts as WriterOptions).rowGroupSize || PARQUET_DEFAULT_ROW_GROUP_SIZE;
    this.closed = false;
    this.userMetadata = {};

    try {
      envelopeWriter.writeHeader();
    } catch (err) {
      envelopeWriter.close();
      throw err;
    }
  }

  /**
   * Append a single row to the parquet file. Rows are buffered in memory until
   * rowGroupSize rows are in the buffer or close() is called
   */
  async appendRow(row: Record<string, unknown>) {
    if (this.closed || this.envelopeWriter === null) {
      throw 'writer was closed';
    }

    parquet_shredder.shredRecord(this.schema, row, this.rowBuffer);

    const options = {
      useDataPageV2: this.envelopeWriter.useDataPageV2,
      bloomFilters: this.envelopeWriter.bloomFilters,
    };
    if (this.rowBuffer.pageRowCount! >= this.envelopeWriter.pageSize) {
      await encodePages(this.schema, this.rowBuffer, options);
    }

    if (this.rowBuffer.rowCount! >= this.rowGroupSize) {
      await encodePages(this.schema, this.rowBuffer, options);
      await this.envelopeWriter.writeRowGroup(this.rowBuffer);
      this.rowBuffer = {};
    }
  }

  /**
   * Finish writing the parquet file and commit the footer to disk. This method
   * MUST be called after you are finished adding rows. You must not call this
   * method twice on the same object or add any rows after the close() method has
   * been called
   */
  async close(callback?: () => void) {
    if (this.closed) {
      throw 'writer was closed';
    }

    this.closed = true;

    if (this.envelopeWriter) {
      if (this.rowBuffer.rowCount! > 0 || this.rowBuffer.rowCount! >= this.rowGroupSize) {
        await encodePages(this.schema, this.rowBuffer, {
          useDataPageV2: this.envelopeWriter.useDataPageV2,
          bloomFilters: this.envelopeWriter.bloomFilters,
        });

        await this.envelopeWriter.writeRowGroup(this.rowBuffer);
        this.rowBuffer = {};
      }

      await this.envelopeWriter.writeBloomFilters();
      await this.envelopeWriter.writeIndex();
      await this.envelopeWriter.writeFooter(this.userMetadata);
      await this.envelopeWriter.close();
      this.envelopeWriter = null;
    }

    if (callback) {
      callback();
    }
  }

  /**
   * Add key<>value metadata to the file
   */
  setMetadata(key: string, value: string) {
    this.userMetadata[key.toString()] = value.toString();
  }

  /**
   * Set the parquet row group size. This values controls the maximum number
   * of rows that are buffered in memory at any given time as well as the number
   * of rows that are co-located on disk. A higher value is generally better for
   * read-time I/O performance at the tradeoff of write-time memory usage.
   */
  setRowGroupSize(cnt: number) {
    this.rowGroupSize = cnt;
  }

  /**
   * Set the parquet data page size. The data page size controls the maximum
   * number of column values that are written to disk as a consecutive array
   */
  setPageSize(cnt: number) {
    this.envelopeWriter!.setPageSize(cnt);
  }
}

/**
 * Create a parquet file from a schema and a number of row groups. This class
 * performs direct, unbuffered writes to the underlying output stream and is
 * intended for advanced and internal users; the writeXXX methods must be
 * called in the correct order to produce a valid file.
 */
export class ParquetEnvelopeWriter {
  schema: ParquetSchema;
  write: (buf: Buffer) => void;
  close: () => void;
  offset: Int64;
  rowCount: Int64;
  rowGroups: RowGroupExt[];
  pageSize: number;
  useDataPageV2: boolean;
  pageIndex: boolean;
  bloomFilters: Record<string, SplitBlockBloomFilter>; // TODO: OR filterCollection

  /**
   * Create a new parquet envelope writer that writes to the specified stream
   */
  static async openStream(schema: ParquetSchema, outputStream: parquet_util.WriteStreamMinimal, opts: WriterOptions) {
    const writeFn = parquet_util.oswrite.bind(undefined, outputStream);
    const closeFn = parquet_util.osend.bind(undefined, outputStream);
    return new ParquetEnvelopeWriter(schema, writeFn, closeFn, new Int64(0), opts);
  }

  constructor(
    schema: ParquetSchema,
    writeFn: (buf: Buffer) => void,
    closeFn: () => void,
    fileOffset: Int64,
    opts: WriterOptions
  ) {
    this.schema = schema;
    this.write = writeFn;
    this.close = closeFn;
    this.offset = fileOffset;
    this.rowCount = new Int64(0);
    this.rowGroups = [];
    this.pageSize = opts.pageSize || PARQUET_DEFAULT_PAGE_SIZE;
    this.useDataPageV2 = 'useDataPageV2' in opts ? opts.useDataPageV2! : true;
    this.pageIndex = opts.pageIndex!;
    this.bloomFilters = {};

    (opts.bloomFilters || []).forEach((bloomOption) => {
      this.bloomFilters[bloomOption.column] = bloomFilterWriter.createSBBF(bloomOption);
    });
  }

  writeSection(buf: Buffer) {
    this.offset.setValue(this.offset.valueOf() + buf.length);
    return this.write(buf);
  }

  /**
   * Encode the parquet file header
   */
  writeHeader() {
    return this.writeSection(Buffer.from(PARQUET_MAGIC));
  }

  /**
   * Encode a parquet row group. The records object should be created using the
   * shredRecord method
   */
  async writeRowGroup(records: parquet_shredder.RecordBuffer) {
    const rgroup = await encodeRowGroup(this.schema, records, {
      baseOffset: this.offset,
      pageSize: this.pageSize,
      useDataPageV2: this.useDataPageV2,
      pageIndex: this.pageIndex,
    });

    this.rowCount.setValue(this.rowCount.valueOf() + records.rowCount!);
    this.rowGroups.push(rgroup.metadata);
    return this.writeSection(rgroup.body);
  }

  writeBloomFilters() {
    this.rowGroups.forEach((group) => {
      group.columns.forEach((column) => {
        if (!column.meta_data?.path_in_schema.length) {
          return;
        }

        const filterName = column.meta_data?.path_in_schema.join(',');
        if (!(filterName in this.bloomFilters)) {
          return;
        }
        const serializedBloomFilterData = bloomFilterWriter.getSerializedBloomFilterData(this.bloomFilters[filterName]);

        bloomFilterWriter.setFilterOffset(column, this.offset);

        this.writeSection(serializedBloomFilterData);
      });
    });
  }

  /**
   * Write the columnIndices and offsetIndices
   */
  writeIndex() {
    this.schema.fieldList.forEach((c, i) => {
      this.rowGroups.forEach((group) => {
        const column = group.columns[i];
        if (!column) return;

        if (column.meta_data?.columnIndex) {
          const columnBody = parquet_util.serializeThrift(column.meta_data.columnIndex);
          delete column.meta_data.columnIndex;
          column.column_index_offset = parquet_util.cloneInteger(this.offset);
          column.column_index_length = columnBody.length;
          this.writeSection(columnBody);
        }

        if (column.meta_data?.offsetIndex) {
          const offsetBody = parquet_util.serializeThrift(column.meta_data.offsetIndex);
          delete column.meta_data.offsetIndex;
          column.offset_index_offset = parquet_util.cloneInteger(this.offset);
          column.offset_index_length = offsetBody.length;
          this.writeSection(offsetBody);
        }
      });
    });
  }

  /**
   * Write the parquet file footer
   */
  writeFooter(userMetadata: Record<string, string>) {
    if (!userMetadata) {
      userMetadata = {};
    }

    if (this.schema.fieldList.length === 0) {
      throw 'cannot write parquet file with zero fieldList';
    }

    return this.writeSection(encodeFooter(this.schema, this.rowCount, this.rowGroups, userMetadata));
  }

  /**
   * Set the parquet data page size. The data page size controls the maximum
   * number of column values that are written to disk as a consecutive array
   */
  setPageSize(cnt: number) {
    this.pageSize = cnt;
  }
}

/**
 * Create a parquet transform stream
 */
export class ParquetTransformer extends stream.Transform {
  writer: ParquetWriter;

  constructor(schema: ParquetSchema, opts = {}) {
    super({ objectMode: true });

    const writeProxy = (function (t) {
      return function (b: unknown) {
        t.push(b);
      };
    })(this);

    this.writer = new ParquetWriter(
      schema,
      new ParquetEnvelopeWriter(
        schema,
        writeProxy,
        () => {
          /* void */
        },
        new Int64(0),
        opts
      ),
      opts
    );
  }

  _transform(row: Record<string, unknown>, _encoding: string, callback: (err?: Error | null, data?: any) => void) {
    if (row) {
      this.writer.appendRow(row).then(
        (data) => callback(null, data),
        (err) => {
          const fullErr = new Error(`Error transforming to parquet: ${err.toString()} row:${row}`);
          fullErr.message = err;
          callback(fullErr);
        }
      );
    } else {
      callback();
    }
  }

  _flush(callback: (foo: any, bar?: any) => any) {
    this.writer.close().then((d) => callback(null, d), callback);
  }
}

/**
 * Encode a consecutive array of data using one of the parquet encodings
 */
function encodeValues(type: string, encoding: ParquetCodec, values: number[], opts: any) {
  if (!(encoding in parquet_codec)) {
    throw 'invalid encoding: ' + encoding;
  }

  return parquet_codec[encoding].encodeValues(type, values, opts);
}

function encodeStatisticsValue(value: any, column: ParquetField | Options) {
  if (value === undefined) {
    return Buffer.alloc(0);
  }
  if (column.originalType) {
    value = parquet_types.toPrimitive(column.originalType, value, column);
  }
  if (column.primitiveType !== 'BYTE_ARRAY') {
    value = encodeValues(column.primitiveType!, 'PLAIN', [value], column);
  }
  return value;
}

function encodeStatistics(statistics: parquet_thrift.Statistics, column: ParquetField | Options) {
  statistics = Object.assign({}, statistics);
  statistics.min_value =
    statistics.min_value === undefined ? null : encodeStatisticsValue(statistics.min_value, column);
  statistics.max_value =
    statistics.max_value === undefined ? null : encodeStatisticsValue(statistics.max_value, column);

  statistics.max = statistics.max_value;
  statistics.min = statistics.min_value;

  return new parquet_thrift.Statistics(statistics);
}

async function encodePages(
  schema: ParquetSchema,
  rowBuffer: parquet_shredder.RecordBuffer,
  opts: { bloomFilters: Record<string, SplitBlockBloomFilter>; useDataPageV2: boolean }
) {
  // generic
  if (!rowBuffer.pageRowCount) {
    return;
  }

  for (const field of schema.fieldList) {
    if (field.isNested) {
      continue;
    }

    let page;

    const columnPath = field.path.join(',');
    const values = rowBuffer.columnData![columnPath];

    if (opts.bloomFilters && columnPath in opts.bloomFilters) {
      const splitBlockBloomFilter = opts.bloomFilters[columnPath];
      values.values!.forEach((v) => splitBlockBloomFilter.insert(v));
    }

    let statistics: parquet_thrift.Statistics = {};
    if (field.statistics !== false) {
      statistics = {};
      [...values.distinct_values!].forEach((v, i) => {
        if (i === 0 || v > statistics.max_value!) {
          statistics.max_value = v;
        }
        if (i === 0 || v < statistics.min_value!) {
          statistics.min_value = v;
        }
      });

      statistics.null_count = new Int64(values.dlevels!.length - values.values!.length);
      statistics.distinct_count = new Int64(values.distinct_values!.size);
    }

    if (opts.useDataPageV2) {
      page = await encodeDataPageV2(
        field,
        values.count!,
        values.values!,
        values.rlevels!,
        values.dlevels!,
        statistics!
      );
    } else {
      page = await encodeDataPage(field, values.values || [], values.rlevels || [], values.dlevels || [], statistics!);
    }

    const pages = rowBuffer.pages![field.path.join(',')];
    const lastPage = pages[pages.length - 1];
    const first_row_index = lastPage ? lastPage.first_row_index + lastPage.count! : 0;
    pages.push({
      page,
      statistics,
      first_row_index,
      distinct_values: values.distinct_values!,
      num_values: values.dlevels!.length,
    });

    values.distinct_values = new Set();
    values.values = [];
    values.rlevels = [];
    values.dlevels = [];
    values.count = 0;
  }

  rowBuffer.pageRowCount = 0;
}

/**
 * Encode a parquet data page
 */
async function encodeDataPage(
  column: ParquetField,
  values: number[],
  rlevels: number[],
  dlevels: number[],
  statistics: parquet_thrift.Statistics
) {
  /* encode values */
  const valuesBuf = encodeValues(column.primitiveType!, column.encoding!, values, {
    bitWidth: column.typeLength,
    ...column,
  });

  /* encode repetition and definition levels */
  let rLevelsBuf = Buffer.alloc(0);
  if (column.rLevelMax > 0) {
    rLevelsBuf = encodeValues(PARQUET_RDLVL_TYPE, PARQUET_RDLVL_ENCODING, rlevels, {
      bitWidth: parquet_util.getBitWidth(column.rLevelMax),
    });
  }

  let dLevelsBuf = Buffer.alloc(0);
  if (column.dLevelMax > 0) {
    dLevelsBuf = encodeValues(PARQUET_RDLVL_TYPE, PARQUET_RDLVL_ENCODING, dlevels, {
      bitWidth: parquet_util.getBitWidth(column.dLevelMax),
    });
  }

  /* build page header */
  let pageBody = Buffer.concat([rLevelsBuf, dLevelsBuf, valuesBuf]);
  pageBody = await parquet_compression.deflate(column.compression!, pageBody);

  const pageHeader = new parquet_thrift.PageHeader();
  pageHeader.type = parquet_thrift.PageType['DATA_PAGE'];
  pageHeader.uncompressed_page_size = rLevelsBuf.length + dLevelsBuf.length + valuesBuf.length;
  pageHeader.compressed_page_size = pageBody.length;
  pageHeader.data_page_header = new parquet_thrift.DataPageHeader();
  pageHeader.data_page_header.num_values = dlevels.length;
  if (column.statistics !== false) {
    pageHeader.data_page_header.statistics = encodeStatistics(statistics, column);
  }

  pageHeader.data_page_header.encoding = parquet_thrift.Encoding[column.encoding!];
  pageHeader.data_page_header.definition_level_encoding = parquet_thrift.Encoding[PARQUET_RDLVL_ENCODING];
  pageHeader.data_page_header.repetition_level_encoding = parquet_thrift.Encoding[PARQUET_RDLVL_ENCODING];

  /* concat page header, repetition and definition levels and values */
  return Buffer.concat([parquet_util.serializeThrift(pageHeader), pageBody]);
}

/**
 * Encode a parquet data page (v2)
 */
async function encodeDataPageV2(
  column: ParquetField,
  rowCount: number,
  values: number[],
  rlevels: number[],
  dlevels: number[],
  statistics: parquet_thrift.Statistics
) {
  /* encode values */
  const valuesBuf = encodeValues(column.primitiveType!, column.encoding!, values, {
    bitWidth: column.typeLength,
    ...column,
  });

  const valuesBufCompressed = await parquet_compression.deflate(column.compression!, valuesBuf);

  /* encode repetition and definition levels */
  let rLevelsBuf = Buffer.alloc(0);
  if (column.rLevelMax > 0) {
    rLevelsBuf = encodeValues(PARQUET_RDLVL_TYPE, PARQUET_RDLVL_ENCODING, rlevels, {
      bitWidth: parquet_util.getBitWidth(column.rLevelMax),
      disableEnvelope: true,
    });
  }

  let dLevelsBuf = Buffer.alloc(0);
  if (column.dLevelMax > 0) {
    dLevelsBuf = encodeValues(PARQUET_RDLVL_TYPE, PARQUET_RDLVL_ENCODING, dlevels, {
      bitWidth: parquet_util.getBitWidth(column.dLevelMax),
      disableEnvelope: true,
    });
  }

  /* build page header */
  const pageHeader = new parquet_thrift.PageHeader();
  pageHeader.type = parquet_thrift.PageType['DATA_PAGE_V2'];
  pageHeader.data_page_header_v2 = new parquet_thrift.DataPageHeaderV2();
  pageHeader.data_page_header_v2.num_values = dlevels.length;
  pageHeader.data_page_header_v2.num_nulls = dlevels.length - values.length;
  pageHeader.data_page_header_v2.num_rows = rowCount;

  if (column.statistics !== false) {
    pageHeader.data_page_header_v2.statistics = encodeStatistics(statistics, column);
  }

  pageHeader.uncompressed_page_size = rLevelsBuf.length + dLevelsBuf.length + valuesBuf.length;

  pageHeader.compressed_page_size = rLevelsBuf.length + dLevelsBuf.length + valuesBufCompressed.length;

  pageHeader.data_page_header_v2.encoding = parquet_thrift.Encoding[column.encoding!];
  pageHeader.data_page_header_v2.definition_levels_byte_length = dLevelsBuf.length;
  pageHeader.data_page_header_v2.repetition_levels_byte_length = rLevelsBuf.length;

  pageHeader.data_page_header_v2.is_compressed = column.compression !== 'UNCOMPRESSED';

  /* concat page header, repetition and definition levels and values */
  return Buffer.concat([parquet_util.serializeThrift(pageHeader), rLevelsBuf, dLevelsBuf, valuesBufCompressed]);
}

/**
 * Encode an array of values into a parquet column chunk
 */
async function encodeColumnChunk(
  pages: Page[],
  opts: {
    column: ParquetField;
    baseOffset: number;
    pageSize: number;
    rowCount: number;
    useDataPageV2: boolean;
    pageIndex: boolean;
  }
) {
  const pagesBuf = Buffer.concat(pages.map((d) => d.page));
  const num_values = pages.reduce((p, d) => p + d.num_values, 0);
  let offset = opts.baseOffset;

  /* prepare metadata header */
  const metadata: ColumnMetaDataExt = new parquet_thrift.ColumnMetaData();
  metadata.path_in_schema = opts.column.path;
  metadata.num_values = new Int64(num_values);
  metadata.data_page_offset = new Int64(opts.baseOffset);
  metadata.encodings = [];
  metadata.total_uncompressed_size = new Int64(pagesBuf.length);
  metadata.total_compressed_size = new Int64(pagesBuf.length);

  metadata.type = parquet_thrift.Type[opts.column.primitiveType!];
  metadata.codec = await parquet_thrift.CompressionCodec[opts.column.compression!];

  /* compile statistics ColumnIndex and OffsetIndex*/
  const columnIndex = new parquet_thrift.ColumnIndex();
  columnIndex.null_pages = [];
  columnIndex.max_values = [];
  columnIndex.min_values = [];
  // Default to unordered
  columnIndex.boundary_order = 0;
  const offsetIndex = new parquet_thrift.OffsetIndex();
  offsetIndex.page_locations = [];

  /* prepare statistics */
  const statistics: parquet_thrift.Statistics = {};
  const distinct_values = new Set();
  statistics.null_count = new Int64(0);
  statistics.distinct_count = new Int64(0);

  /* loop through pages and update indices and statistics */
  for (let i = 0; i < pages.length; i++) {
    const page = pages[i];

    if (opts.column.statistics !== false) {
      if (page.statistics.max_value! > statistics.max_value! || i == 0) {
        statistics.max_value = page.statistics.max_value;
      }
      if (page.statistics.min_value! < statistics.min_value! || i == 0) {
        statistics.min_value = page.statistics.min_value;
      }
      statistics.null_count.setValue(statistics.null_count.valueOf() + (page.statistics.null_count?.valueOf() || 0));
      page.distinct_values.forEach((value: unknown) => distinct_values.add(value));

      // If the number of values and the count of nulls are the same, this is a null page
      columnIndex.null_pages.push(page.num_values === statistics.null_count.valueOf());
      columnIndex.max_values.push(encodeStatisticsValue(page.statistics.max_value, opts.column));
      columnIndex.min_values.push(encodeStatisticsValue(page.statistics.min_value, opts.column));
    }

    const pageLocation = new parquet_thrift.PageLocation();
    pageLocation.offset = new Int64(offset);
    offset += page.page.length;
    pageLocation.compressed_page_size = page.page.length;
    pageLocation.first_row_index = new Int64(page.first_row_index);
    offsetIndex.page_locations.push(pageLocation);
  }

  if (opts.pageIndex !== false) {
    metadata.offsetIndex = offsetIndex;
  }

  if (opts.column.statistics !== false) {
    statistics.distinct_count = new Int64(distinct_values.size);
    metadata.statistics = encodeStatistics(statistics, opts.column);
    if (opts.pageIndex !== false) {
      metadata.columnIndex = columnIndex;
    }
  }

  /* list encodings */
  metadata.encodings.push(parquet_thrift.Encoding[PARQUET_RDLVL_ENCODING]);
  metadata.encodings.push(parquet_thrift.Encoding[opts.column.encoding!]);

  /* concat metadata header and data pages */
  const metadataOffset = opts.baseOffset + pagesBuf.length;
  const body = Buffer.concat([pagesBuf, parquet_util.serializeThrift(metadata)]);
  return { body, metadata, metadataOffset };
}

/**
 * Encode a list of column values into a parquet row group
 */
async function encodeRowGroup(schema: ParquetSchema, data: parquet_shredder.RecordBuffer, opts: WriterOptions) {
  const metadata: RowGroupExt = new parquet_thrift.RowGroup();
  metadata.num_rows = new Int64(data.rowCount!);
  metadata.columns = [];
  metadata.total_byte_size = new Int64(0);

  let body = Buffer.alloc(0);
  for (const field of schema.fieldList) {
    if (field.isNested) {
      continue;
    }

    const cchunkData = await encodeColumnChunk(data.pages![field.path.join(',')], {
      column: field,
      baseOffset: opts.baseOffset!.valueOf() + body.length,
      pageSize: opts.pageSize || 0,
      rowCount: data.rowCount || 0,
      useDataPageV2: opts.useDataPageV2 ?? true,
      pageIndex: opts.pageIndex ?? true,
    });

    const cchunk = new parquet_thrift.ColumnChunk();
    cchunk.file_offset = new Int64(cchunkData.metadataOffset);
    cchunk.meta_data = cchunkData.metadata;
    metadata.columns.push(cchunk);
    metadata.total_byte_size = new Int64(metadata.total_byte_size.valueOf() + cchunkData.body.length);

    body = Buffer.concat([body, cchunkData.body]);
  }

  return { body, metadata };
}

/**
 * Encode a parquet file metadata footer
 */
function encodeFooter(
  schema: ParquetSchema,
  rowCount: Int64,
  rowGroups: RowGroupExt[],
  userMetadata: Record<string, string>
) {
  const metadata = new parquet_thrift.FileMetaData();
  metadata.version = PARQUET_VERSION;
  metadata.created_by = '@dsnp/parquetjs';
  metadata.num_rows = rowCount;
  metadata.row_groups = rowGroups;
  metadata.schema = [];
  metadata.key_value_metadata = [];

  for (const k in userMetadata) {
    const kv = new parquet_thrift.KeyValue();
    kv.key = k;
    kv.value = userMetadata[k];
    metadata.key_value_metadata.push(kv);
  }

  {
    const schemaRoot = new parquet_thrift.SchemaElement();
    schemaRoot.name = 'root';
    schemaRoot.num_children = Object.keys(schema.fields).length;
    metadata.schema.push(schemaRoot);
  }

  for (const field of schema.fieldList) {
    const schemaElem = new parquet_thrift.SchemaElement();
    schemaElem.name = field.name;
    schemaElem.repetition_type = parquet_thrift.FieldRepetitionType[field.repetitionType];

    if (field.isNested) {
      schemaElem.num_children = field.fieldCount;
    } else {
      schemaElem.type = parquet_thrift.Type[field.primitiveType!];
    }

    if (field.originalType) {
      schemaElem.converted_type = parquet_thrift.ConvertedType[field.originalType];
    }

    // Support Decimal
    switch (schemaElem.converted_type) {
      case ConvertedType.DECIMAL:
        schemaElem.precision = field.precision;
        schemaElem.scale = field.scale || 0;
        break;
    }

    schemaElem.type_length = field.typeLength;

    metadata.schema.push(schemaElem);
  }

  const metadataEncoded = parquet_util.serializeThrift(metadata);
  const footerEncoded = Buffer.alloc(metadataEncoded.length + 8);
  metadataEncoded.copy(footerEncoded);
  footerEncoded.writeUInt32LE(metadataEncoded.length, metadataEncoded.length);
  footerEncoded.write(PARQUET_MAGIC, metadataEncoded.length + 4);
  return footerEncoded;
}
