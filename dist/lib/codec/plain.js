"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.decodeValues = exports.encodeValues = void 0;
const int53_1 = __importDefault(require("int53"));
function encodeValues_BOOLEAN(values) {
    const buf = Buffer.allocUnsafe(Math.ceil(values.length / 8));
    buf.fill(0);
    for (let i = 0; i < values.length; ++i) {
        if (values[i]) {
            buf[Math.floor(i / 8)] |= 1 << i % 8;
        }
    }
    return buf;
}
function decodeValues_BOOLEAN(cursor, count) {
    const values = [];
    for (let i = 0; i < count; ++i) {
        const b = cursor.buffer[cursor.offset + Math.floor(i / 8)];
        values.push((b & (1 << i % 8)) > 0);
    }
    cursor.offset += Math.ceil(count / 8);
    return values;
}
function encodeValues_INT32(values, opts) {
    const isDecimal = opts?.originalType === 'DECIMAL' || opts?.column?.originalType === 'DECIMAL';
    const scale = opts?.scale || 0;
    const buf = Buffer.allocUnsafe(4 * values.length);
    if (isDecimal) {
        const multiplier = Math.pow(10, scale);
        for (let i = 0; i < values.length; i++) {
            buf.writeInt32LE(values[i] * multiplier, i * 4);
        }
    }
    else {
        for (let i = 0; i < values.length; i++) {
            buf.writeInt32LE(values[i], i * 4);
        }
    }
    return buf;
}
function decodeValues_INT32(cursor, count, opts) {
    let values = [];
    const name = opts.name || opts.column?.name || undefined;
    try {
        if (opts.originalType === 'DECIMAL') {
            values = decodeValues_DECIMAL(cursor, count, opts);
        }
        else {
            for (let i = 0; i < count; ++i) {
                values.push(cursor.buffer.readInt32LE(cursor.offset));
                cursor.offset += 4;
            }
        }
    }
    catch (e) {
        console.log(`Error thrown for column: ${name}`);
        throw e;
    }
    return values;
}
function encodeValues_INT64(values, opts) {
    const isDecimal = opts?.originalType === 'DECIMAL' || opts?.column?.originalType === 'DECIMAL';
    const scale = opts?.scale || 0;
    const buf = Buffer.allocUnsafe(8 * values.length);
    if (isDecimal) {
        const multiplier = Math.pow(10, scale);
        for (let i = 0; i < values.length; i++) {
            const value = values[i];
            const bigIntValue = typeof value === 'bigint' ? value : BigInt(Math.floor(value * multiplier));
            buf.writeBigInt64LE(bigIntValue, i * 8);
        }
    }
    else {
        for (let i = 0; i < values.length; i++) {
            const value = values[i];
            const bigIntValue = typeof value === 'bigint' ? value : BigInt(value);
            buf.writeBigInt64LE(bigIntValue, i * 8);
        }
    }
    return buf;
}
function decodeValues_INT64(cursor, count, opts) {
    let values = [];
    const name = opts.name || opts.column?.name || undefined;
    try {
        if (opts.originalType === 'DECIMAL' || opts.column?.originalType === 'DECIMAL') {
            const columnOptions = opts.column?.originalType ? opts.column : opts;
            values = decodeValues_DECIMAL(cursor, count, columnOptions);
        }
        else {
            for (let i = 0; i < count; ++i) {
                values.push(cursor.buffer.readBigInt64LE(cursor.offset));
                cursor.offset += 8;
            }
        }
    }
    catch (e) {
        console.log(`Error thrown for column: ${name}`);
        throw e;
    }
    return values;
}
function decodeValues_DECIMAL(cursor, count, opts) {
    const precision = opts.precision;
    // Default scale to 0 per spec
    const scale = opts.scale || 0;
    const name = opts.name || undefined;
    if (!precision) {
        throw new Error(`missing option: precision (required for DECIMAL) for column: ${name}`);
    }
    const values = [];
    // by default we prepare the offset and bufferFunction to work with 32bit integers
    let offset = 4;
    let bufferFunction = (offset) => cursor.buffer.readInt32LE(offset);
    if (precision > 9) {
        // if the precision is over 9 digits, then we are dealing with a 64bit integer
        offset = 8;
        bufferFunction = (offset) => cursor.buffer.readBigInt64LE(offset);
    }
    for (let i = 0; i < count; ++i) {
        const bufferSize = cursor.size || 0;
        if (bufferSize === 0 || cursor.offset < bufferSize) {
            const fullValue = bufferFunction(cursor.offset);
            const valueWithDecimalApplied = Number(fullValue) / Math.pow(10, scale);
            values.push(valueWithDecimalApplied);
            cursor.offset += offset;
        }
    }
    return values;
}
function encodeValues_INT96(values) {
    const buf = Buffer.allocUnsafe(12 * values.length);
    for (let i = 0; i < values.length; i++) {
        if (values[i] >= 0) {
            int53_1.default.writeInt64LE(values[i], buf, i * 12);
            buf.writeUInt32LE(0, i * 12 + 8); // truncate to 64 actual precision
        }
        else {
            int53_1.default.writeInt64LE(~-values[i] + 1, buf, i * 12);
            buf.writeUInt32LE(0xffffffff, i * 12 + 8); // truncate to 64 actual precision
        }
    }
    return buf;
}
function decodeValues_INT96(cursor, count, opts) {
    const values = [];
    // Default to false for backward compatibility
    const treatAsTimestamp = opts?.treatInt96AsTimestamp === true;
    for (let i = 0; i < count; ++i) {
        // when treatAsTimestamp is true, low is nanoseconds since midnight
        const low = int53_1.default.readInt64LE(cursor.buffer, cursor.offset);
        // when treatAsTimestamp is true, high is Julian day
        const high = cursor.buffer.readUInt32LE(cursor.offset + 8);
        if (treatAsTimestamp) {
            // Convert Julian day and nanoseconds to a timestamp
            values.push(convertInt96ToTimestamp(high, low));
        }
        else {
            // For non-timestamp INT96 values, maintain existing behavior
            if (high === 0xffffffff) {
                values.push(~-low + 1); // negative value
            }
            else {
                values.push(low); // positive value
            }
        }
        cursor.offset += 12;
    }
    return values;
}
/**
 * Convert INT96 to timestamp
 * In the Parquet format, INT96 timestamps are stored as:
 * - The first 8 bytes (low) represent nanoseconds within the day
 * - The last 4 bytes (high) represent the Julian day
 *
 * @param julianDay Julian day number
 * @param nanosSinceMidnight Nanoseconds since midnight
 * @returns JavaScript Date object (UTC)
 */
function convertInt96ToTimestamp(julianDay, nanosSinceMidnight) {
    // Julian day 2440588 corresponds to 1970-01-01 (Unix epoch)
    const daysSinceEpoch = julianDay - 2440588;
    // Convert days to milliseconds (86,400,000 ms per day)
    const millisSinceEpoch = daysSinceEpoch * 86400000;
    // Convert nanoseconds to milliseconds
    const nanosInMillis = Number(BigInt(nanosSinceMidnight) / 1000000n);
    // Create a UTC Date
    return new Date(millisSinceEpoch + nanosInMillis);
}
function encodeValues_FLOAT(values) {
    const buf = Buffer.allocUnsafe(4 * values.length);
    for (let i = 0; i < values.length; i++) {
        buf.writeFloatLE(values[i], i * 4);
    }
    return buf;
}
function decodeValues_FLOAT(cursor, count) {
    const values = [];
    for (let i = 0; i < count; ++i) {
        values.push(cursor.buffer.readFloatLE(cursor.offset));
        cursor.offset += 4;
    }
    return values;
}
function encodeValues_DOUBLE(values) {
    const buf = Buffer.allocUnsafe(8 * values.length);
    for (let i = 0; i < values.length; i++) {
        buf.writeDoubleLE(values[i], i * 8);
    }
    return buf;
}
function decodeValues_DOUBLE(cursor, count) {
    const values = [];
    for (let i = 0; i < count; ++i) {
        values.push(cursor.buffer.readDoubleLE(cursor.offset));
        cursor.offset += 8;
    }
    return values;
}
const STR_PAD = 8;
function _slowEncodeBufferEnd_BYTE_ARRAY(values, begin_buf, fromIndex) {
    let rest_len = 0;
    for (let j = fromIndex; j < values.length; j++) {
        const x = values[j];
        rest_len += 4 + (typeof x === 'string' ? Buffer.byteLength(x, 'utf8') : x.length);
    }
    const rest_buf = Buffer.allocUnsafe(rest_len);
    for (let j = fromIndex, rest_pos = 0; j < values.length; j++) {
        const x = values[j];
        if (typeof x === 'string') {
            const l = rest_buf.write(x, rest_pos + 4, 'utf8');
            rest_buf.writeUInt32LE(l, rest_pos);
            rest_pos += 4 + l;
        }
        else {
            const l = x.length;
            rest_buf.writeUInt32LE(l, rest_pos);
            if (Buffer.isBuffer(x)) {
                x.copy(rest_buf, rest_pos + 4);
            }
            else {
                rest_buf.set(x, rest_pos + 4);
            }
            rest_pos += 4 + l;
        }
    }
    return Buffer.concat([begin_buf, rest_buf]);
}
function encodeValues_BYTE_ARRAY(values) {
    let buf_len = 0;
    for (let i = 0; i < values.length; i++) {
        const v = values[i];
        buf_len += 4 + v.length + (typeof v === 'string' ? STR_PAD : 0);
    }
    const buf = Buffer.allocUnsafe(buf_len);
    let buf_pos = 0;
    let i = 0;
    try {
        for (; i < values.length; i++) {
            const v = values[i];
            if (typeof v === 'string') {
                const len = buf.write(v, buf_pos + 4, 'utf8');
                buf.writeUInt32LE(len, buf_pos);
                buf_pos += 4 + len;
            }
            else {
                buf.writeUInt32LE(v.length, buf_pos);
                if (Buffer.isBuffer(v)) {
                    v.copy(buf, buf_pos + 4);
                }
                else {
                    buf.set(v, buf_pos + 4);
                }
                buf_pos += 4 + v.length;
            }
        }
    }
    catch (e) {
        if (e instanceof RangeError) {
            return _slowEncodeBufferEnd_BYTE_ARRAY(values, buf.subarray(0, buf_pos), i);
        }
        throw e;
    }
    return buf_pos < buf.length ? buf.subarray(0, buf_pos) : buf;
}
function decodeValues_BYTE_ARRAY(cursor, count) {
    const values = [];
    for (let i = 0; i < count; ++i) {
        const len = cursor.buffer.readUInt32LE(cursor.offset);
        cursor.offset += 4;
        values.push(cursor.buffer.subarray(cursor.offset, cursor.offset + len));
        cursor.offset += len;
    }
    return values;
}
function encodeValues_FIXED_LEN_BYTE_ARRAY(values, opts) {
    if (!opts.typeLength) {
        throw new Error('missing option: typeLength (required for FIXED_LEN_BYTE_ARRAY)');
    }
    const typeLength = opts.typeLength;
    const buf = Buffer.allocUnsafe(typeLength * values.length);
    for (let i = 0; i < values.length; i++) {
        const value = values[i];
        const offset = i * typeLength;
        if (typeof value === 'string') {
            const stringBuf = Buffer.from(value, 'utf8');
            if (stringBuf.length !== typeLength) {
                throw new Error('invalid value for FIXED_LEN_BYTE_ARRAY: ' + value);
            }
            stringBuf.copy(buf, offset);
        }
        else {
            if (value.length !== typeLength) {
                throw new Error('invalid value for FIXED_LEN_BYTE_ARRAY: ' + value);
            }
            if (Buffer.isBuffer(value)) {
                value.copy(buf, offset);
            }
            else {
                buf.set(value, offset);
            }
        }
    }
    return buf;
}
function decodeValues_FIXED_LEN_BYTE_ARRAY(cursor, count, opts) {
    const values = [];
    const typeLength = opts.typeLength ?? (opts.column ? opts.column.typeLength : undefined);
    if (!typeLength) {
        throw new Error('missing option: typeLength (required for FIXED_LEN_BYTE_ARRAY)');
    }
    for (let i = 0; i < count; ++i) {
        values.push(cursor.buffer.subarray(cursor.offset, cursor.offset + typeLength));
        cursor.offset += typeLength;
    }
    return values;
}
const encodeValues = function (type, values, opts) {
    switch (type) {
        case 'BOOLEAN':
            return encodeValues_BOOLEAN(values);
        case 'INT32':
            return encodeValues_INT32(values, opts);
        case 'INT64':
            return encodeValues_INT64(values, opts);
        case 'INT96':
            return encodeValues_INT96(values);
        case 'FLOAT':
            return encodeValues_FLOAT(values);
        case 'DOUBLE':
            return encodeValues_DOUBLE(values);
        case 'BYTE_ARRAY':
            return encodeValues_BYTE_ARRAY(values);
        case 'FIXED_LEN_BYTE_ARRAY':
            return encodeValues_FIXED_LEN_BYTE_ARRAY(values, opts);
        default:
            throw new Error('unsupported type: ' + type);
    }
};
exports.encodeValues = encodeValues;
const decodeValues = function (type, cursor, count, opts) {
    switch (type) {
        case 'BOOLEAN':
            return decodeValues_BOOLEAN(cursor, count);
        case 'INT32':
            return decodeValues_INT32(cursor, count, opts);
        case 'INT64':
            return decodeValues_INT64(cursor, count, opts);
        case 'INT96':
            return decodeValues_INT96(cursor, count, opts);
        case 'FLOAT':
            return decodeValues_FLOAT(cursor, count);
        case 'DOUBLE':
            return decodeValues_DOUBLE(cursor, count);
        case 'BYTE_ARRAY':
            return decodeValues_BYTE_ARRAY(cursor, count);
        case 'FIXED_LEN_BYTE_ARRAY':
            return decodeValues_FIXED_LEN_BYTE_ARRAY(cursor, count, opts);
        default:
            throw new Error('unsupported type: ' + type);
    }
};
exports.decodeValues = decodeValues;
