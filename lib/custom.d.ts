declare module 'int53' {
  export const writeInt64LE: (value: number, buf: Buffer, num: number) => void;
  export const readInt64LE: (buf: Buffer, offset: number) => number;
}

declare module 'snappyjs' {
  export const compress: (value: ArrayBuffer | Buffer | Uint8Array) => ArrayBuffer | Buffer | Uint8Array;
  export const uncompress: (value: ArrayBuffer | Buffer | Uint8Array) => ArrayBuffer | Buffer | Uint8Array;
}
