import INT53 from "int53";
import { Cursor, Options } from "./types";

function encodeValues_BOOLEAN(values: Array<boolean>) {
  let buf = Buffer.alloc(Math.ceil(values.length / 8));
  buf.fill(0);

  for (let i = 0; i < values.length; ++i) {
    if (values[i]) {
      buf[Math.floor(i / 8)] |= 1 << i % 8;
    }
  }

  return buf;
}

function decodeValues_BOOLEAN(cursor: Cursor, count: number) {
  let values = [];

  for (let i = 0; i < count; ++i) {
    let b = cursor.buffer[cursor.offset + Math.floor(i / 8)];
    values.push((b & (1 << i % 8)) > 0);
  }

  cursor.offset += Math.ceil(count / 8);
  return values;
}

function encodeValues_INT32(values: Array<number>) {
  let buf = Buffer.alloc(4 * values.length);
  for (let i = 0; i < values.length; i++) {
    buf.writeInt32LE(values[i], i * 4);
  }

  return buf;
}

function decodeValues_INT32(cursor: Cursor, count: number) {
  let values = [];

  for (let i = 0; i < count; ++i) {
    values.push(cursor.buffer.readInt32LE(cursor.offset));
    cursor.offset += 4;
  }

  return values;
}

function encodeValues_INT64(values: Array<number>) {
  let buf = Buffer.alloc(8 * values.length);
  for (let i = 0; i < values.length; i++) {
    buf.writeBigInt64LE(BigInt(values[i]), i * 8);
  }

  return buf;
}

function decodeValues_INT64(cursor: Cursor, count: number) {
  let values = [];

  for (let i = 0; i < count; ++i) {
    values.push(cursor.buffer.readBigInt64LE(cursor.offset));
    cursor.offset += 8;
  }

  return values;
}

function encodeValues_INT96(values: Array<number>) {
  let buf = Buffer.alloc(12 * values.length);

  for (let i = 0; i < values.length; i++) {
    if (values[i] >= 0) {
      INT53.writeInt64LE(values[i], buf, i * 12);
      buf.writeUInt32LE(0, i * 12 + 8); // truncate to 64 actual precision
    } else {
      INT53.writeInt64LE(~-values[i] + 1, buf, i * 12);
      buf.writeUInt32LE(0xffffffff, i * 12 + 8); // truncate to 64 actual precision
    }
  }

  return buf;
}

function decodeValues_INT96(cursor: Cursor, count: number) {
  let values = [];

  for (let i = 0; i < count; ++i) {
    const low = INT53.readInt64LE(cursor.buffer, cursor.offset);
    const high = cursor.buffer.readUInt32LE(cursor.offset + 8);

    if (high === 0xffffffff) {
      values.push(~-low + 1); // truncate to 64 actual precision
    } else {
      values.push(low); // truncate to 64 actual precision
    }

    cursor.offset += 12;
  }

  return values;
}

function encodeValues_FLOAT(values: Array<number>) {
  let buf = Buffer.alloc(4 * values.length);
  for (let i = 0; i < values.length; i++) {
    buf.writeFloatLE(values[i], i * 4);
  }

  return buf;
}

function decodeValues_FLOAT(cursor: Cursor, count: number) {
  let values = [];

  for (let i = 0; i < count; ++i) {
    values.push(cursor.buffer.readFloatLE(cursor.offset));
    cursor.offset += 4;
  }

  return values;
}

function encodeValues_DOUBLE(values: Array<number>) {
  let buf = Buffer.alloc(8 * values.length);
  for (let i = 0; i < values.length; i++) {
    buf.writeDoubleLE(values[i], i * 8);
  }

  return buf;
}

function decodeValues_DOUBLE(cursor: Cursor, count: number) {
  let values = [];

  for (let i = 0; i < count; ++i) {
    values.push(cursor.buffer.readDoubleLE(cursor.offset));
    cursor.offset += 8;
  }

  return values;
}

// Waylands reminder to check again
function encodeValues_BYTE_ARRAY(values: Array<Uint8Array>) {
  let buf_len = 0;
  const returnedValues: Array<Buffer> = [];
  for (let i = 0; i < values.length; i++) {
    returnedValues[i] = Buffer.from(values[i]);
    buf_len += 4 + returnedValues[i].length;
  }

  let buf = Buffer.alloc(buf_len);
  let buf_pos = 0;
  for (let i = 0; i < returnedValues.length; i++) {
    buf.writeUInt32LE(returnedValues[i].length, buf_pos);
    returnedValues[i].copy(buf, buf_pos + 4);
    buf_pos += 4 + returnedValues[i].length;
  }

  return buf;
}

function decodeValues_BYTE_ARRAY(cursor: Cursor, count: number) {
  let values = [];

  for (let i = 0; i < count; ++i) {
    let len = cursor.buffer.readUInt32LE(cursor.offset);
    cursor.offset += 4;
    values.push(cursor.buffer.slice(cursor.offset, cursor.offset + len));
    cursor.offset += len;
  }

  return values;
}

function encodeValues_FIXED_LEN_BYTE_ARRAY(
  values: Array<Uint8Array | string>,
  opts: Options
) {
  if (!opts.typeLength) {
    throw "missing option: typeLength (required for FIXED_LEN_BYTE_ARRAY)";
  }

  const returnedValues: Array<Buffer> = [];
  for (let i = 0; i < values.length; i++) {
    returnedValues[i] = Buffer.from(values[i]);

    if (returnedValues[i].length !== opts.typeLength) {
      throw "invalid value for FIXED_LEN_BYTE_ARRAY: " + returnedValues[i];
    }
  }

  return Buffer.concat(returnedValues);
}

function decodeValues_FIXED_LEN_BYTE_ARRAY(
  cursor: Cursor,
  count: number,
  opts: Options
) {
  let values = [];

  if (!opts.typeLength) {
    throw "missing option: typeLength (required for FIXED_LEN_BYTE_ARRAY)";
  }

  for (let i = 0; i < count; ++i) {
    values.push(
      cursor.buffer.slice(cursor.offset, cursor.offset + opts.typeLength)
    );
    cursor.offset += opts.typeLength;
  }

  return values;
}

export const encodeValues = function (
  type: string,
  values: Array<unknown>,
  opts: Options
) {
  switch (type) {
    case "BOOLEAN":
      return encodeValues_BOOLEAN(values as Array<boolean>);

    case "INT32":
      return encodeValues_INT32(values as Array<number>);

    case "INT64":
      return encodeValues_INT64(values as Array<number>);

    case "INT96":
      return encodeValues_INT96(values as Array<number>);

    case "FLOAT":
      return encodeValues_FLOAT(values as Array<number>);

    case "DOUBLE":
      return encodeValues_DOUBLE(values as Array<number>);

    case "BYTE_ARRAY":
      return encodeValues_BYTE_ARRAY(values as Array<Uint8Array>);

    case "FIXED_LEN_BYTE_ARRAY":
      return encodeValues_FIXED_LEN_BYTE_ARRAY(
        values as Array<Uint8Array | string>,
        opts
      );

    default:
      throw "unsupported type: " + type;
  }
};

export const decodeValues = function (
  type: string,
  cursor: Cursor,
  count: number,
  opts: Options
) {
  switch (type) {
    case "BOOLEAN":
      return decodeValues_BOOLEAN(cursor, count);

    case "INT32":
      return decodeValues_INT32(cursor, count);

    case "INT64":
      return decodeValues_INT64(cursor, count);

    case "INT96":
      return decodeValues_INT96(cursor, count);

    case "FLOAT":
      return decodeValues_FLOAT(cursor, count);

    case "DOUBLE":
      return decodeValues_DOUBLE(cursor, count);

    case "BYTE_ARRAY":
      return decodeValues_BYTE_ARRAY(cursor, count);

    case "FIXED_LEN_BYTE_ARRAY":
      return decodeValues_FIXED_LEN_BYTE_ARRAY(cursor, count, opts);

    default:
      throw "unsupported type: " + type;
  }
};
