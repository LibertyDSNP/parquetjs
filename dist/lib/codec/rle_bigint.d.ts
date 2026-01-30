interface Cursor {
    buffer: Buffer;
    offset: number;
}
export declare const decodeValuesBigInt: (type: string, cursor: Cursor, count: number, opts: {
    bitWidth: number;
    disableEnvelope?: boolean;
}) => bigint[];
export declare const encodeValuesBigInt: (type: string, values: (number | bigint)[], opts: {
    bitWidth: number;
    disableEnvelope?: boolean;
}) => Buffer<ArrayBuffer>;
export {};
