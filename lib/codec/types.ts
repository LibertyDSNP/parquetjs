export interface Options {
    typeLength: number,
    bitWidth: number,
    disableEnvelope: boolean
}
  
export interface Cursor {
    buffer: Buffer,
    offset: number,
}