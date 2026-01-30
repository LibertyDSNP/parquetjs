"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.encodeValuesBigInt = exports.decodeValuesBigInt = void 0;
const varint_1 = __importDefault(require("varint"));
function encodeRunBitpacked(values, opts) {
    for (let i = 0; i < values.length % 8; i++) {
        values.push(0n);
    }
    const buf = Buffer.alloc(Math.ceil(opts.bitWidth * (values.length / 8)));
    for (let b = 0; b < opts.bitWidth * values.length; ++b) {
        if ((values[Math.floor(b / opts.bitWidth)] & (1n << BigInt(b % opts.bitWidth))) > 0n) {
            buf[Math.floor(b / 8)] |= 1 << b % 8;
        }
    }
    return Buffer.concat([Buffer.from(varint_1.default.encode(((values.length / 8) << 1) | 1)), buf]);
}
function encodeRunRepeated(value, count, opts) {
    const buf = Buffer.alloc(Math.ceil(opts.bitWidth / 8));
    let remainingValue = value;
    for (let i = 0; i < buf.length; ++i) {
        buf.writeUInt8(Number(remainingValue & 0xffn), i);
        remainingValue = remainingValue >> 8n;
    }
    return Buffer.concat([Buffer.from(varint_1.default.encode(count << 1)), buf]);
}
function unknownToParsedBigInt(value) {
    if (typeof value === 'string') {
        return BigInt(value);
    }
    else if (typeof value === 'number') {
        return BigInt(value);
    }
    else {
        return value;
    }
}
function decodeRunBitpackedBigInt(cursor, count, opts) {
    if (count % 8 !== 0) {
        throw new Error('must be a multiple of 8');
    }
    const values = new Array(count).fill(0n);
    for (let b = 0; b < opts.bitWidth * count; ++b) {
        if (cursor.buffer[cursor.offset + Math.floor(b / 8)] & (1 << b % 8)) {
            values[Math.floor(b / opts.bitWidth)] |= 1n << BigInt(b % opts.bitWidth);
        }
    }
    cursor.offset += Math.ceil(opts.bitWidth * (count / 8));
    return values;
}
function decodeRunRepeatedBigInt(cursor, count, opts) {
    const bytesNeededForFixedBitWidth = Math.ceil(opts.bitWidth / 8);
    let value = 0n;
    for (let i = 0; i < bytesNeededForFixedBitWidth; ++i) {
        const byte = cursor.buffer[cursor.offset];
        value += BigInt(byte) << BigInt(i * 8);
        cursor.offset += 1;
    }
    return new Array(count).fill(value);
}
const decodeValuesBigInt = function (type, cursor, count, opts) {
    if (!('bitWidth' in opts)) {
        throw new Error('bitWidth is required');
    }
    if (!opts.disableEnvelope) {
        cursor.offset += 4;
    }
    let values = [];
    let res;
    while (values.length < count) {
        const header = varint_1.default.decode(cursor.buffer, cursor.offset);
        cursor.offset += varint_1.default.encodingLength(header);
        if (header & 1) {
            res = decodeRunBitpackedBigInt(cursor, (header >> 1) * 8, opts);
        }
        else {
            res = decodeRunRepeatedBigInt(cursor, header >> 1, opts);
        }
        for (let i = 0; i < res.length; i++) {
            values.push(res[i]);
        }
    }
    values = values.slice(0, count);
    if (values.length !== count) {
        throw new Error('invalid RLE encoding');
    }
    return values;
};
exports.decodeValuesBigInt = decodeValuesBigInt;
const encodeValuesBigInt = function (type, values, opts) {
    if (!('bitWidth' in opts)) {
        throw new Error('bitWidth is required');
    }
    let bigintValues;
    switch (type) {
        case 'BOOLEAN':
        case 'INT32':
        case 'INT64':
            bigintValues = values.map((x) => unknownToParsedBigInt(x));
            break;
        default:
            throw new Error('unsupported type: ' + type);
    }
    let buf = Buffer.alloc(0);
    let run = [];
    let repeats = 0;
    for (let i = 0; i < bigintValues.length; i++) {
        if (repeats === 0 && run.length % 8 === 0 && bigintValues[i] === bigintValues[i + 1]) {
            if (run.length) {
                buf = Buffer.concat([buf, encodeRunBitpacked(run, opts)]);
                run = [];
            }
            repeats = 1;
        }
        else if (repeats > 0 && bigintValues[i] === bigintValues[i - 1]) {
            repeats += 1;
        }
        else {
            if (repeats) {
                buf = Buffer.concat([buf, encodeRunRepeated(bigintValues[i - 1], repeats, opts)]);
                repeats = 0;
            }
            run.push(bigintValues[i]);
        }
    }
    if (repeats) {
        buf = Buffer.concat([buf, encodeRunRepeated(bigintValues[bigintValues.length - 1], repeats, opts)]);
    }
    else if (run.length) {
        buf = Buffer.concat([buf, encodeRunBitpacked(run, opts)]);
    }
    if (opts.disableEnvelope) {
        return buf;
    }
    const envelope = Buffer.alloc(buf.length + 4);
    envelope.writeUInt32LE(buf.length);
    buf.copy(envelope, 4);
    return envelope;
};
exports.encodeValuesBigInt = encodeValuesBigInt;
