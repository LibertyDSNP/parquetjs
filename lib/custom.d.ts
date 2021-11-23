
declare module 'int53' {
    export const writeInt64LE: (value: number, buf: Buffer, num: number) => {}
    export const readInt64LE: (buf: Buffer, offset: number) => {} 
}

declare module 'snappyjs' {
    export const compress: (value: ArrayBuffer | Buffer | Uint8Array) => {}
    export const uncompress: (value: ArrayBuffer | Buffer | Uint8Array) => {}
}

