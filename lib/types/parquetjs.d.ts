// <reference types = "parquetjs" />

declare module "parquetjs" {

    export class ParquetEnvelopeReader {
        getCursor(columnList?: any): any
    }
    export class ParquetReader {
        static openFile(filePath: string, options?: any): any

        static openUrl(request: any, params: any, options?: any): any
    }
    type schemaPath = string|Array<string>
    export class ParquetSchema {
        constructor(schema:any);
        findField(path:schemaPath):any
        findFieldBranch(path:any):any
        buildFields(schema:ParquetSchema,
                    rLevelParentMax:number,
                    dLevelParentMax:number,
                    path:schemaPath):any
        listFields(fields: Array<string>):any
    }

    export class ParquetEnvelopeWriter{
        static openStream(schema:ParquetSchema, outputStream:Buffer, opts?:any): any

        constructor(schema:ParquetSchema, writeFn:any, closeFn:any, fileOffset:number, opts?:any)

        writeSection(buf: Array<any>):any
        writeHeader():void
        writeRowGroup(records: Array<any>): any;
        writeIndex(_rowGroups: any): any
        writeFooter(userMetadata: any, schema: ParquetSchema, rowCount:number, rowGroups:any):any
    }

    export class ParquetWriter{
        static openFile(schema:ParquetSchema, path:schemaPath, opts?:any):ParquetWriter
        static openStream(schema:ParquetSchema, outputStream: Buffer, opts?:any): ParquetWriter

        constructor(schema: ParquetSchema, envelopeWriter:ParquetEnvelopeWriter, opts?:any)
        appendRow(row: any): void
        close(callback?: Function):void
        setMetadata(key:any, value:any):void
        setRowGroupSize(cnt:number): void
        setPageSize(cnt:number): void
    }
    export type Block = Uint32Array;

    export class SplitBlockBloomFilter {
        static getBlockIndex(h: Long, z: number): number
        static optimalNumOfBits(numDistinct: Long, falsePositiveProbability: number): number
        static mask(hashValue: number): Block
        static blockInsert(b: Block, hashValue: number): void
        static blockCheck(b: Block, hashValue: number): boolean
        setOptFalsePositiveRate(percent: number): SplitBlockBloomFilter
        setOptNumBits(numBits: number): SplitBlockBloomFilter
        init(): SplitBlockBloomFilter
        filter(): Array<Block>
        numDistinctExpected(): number
        falsePositiveRate(): number
        getNumFilterBlocks(): number
        getNumFilterBytes(): number
        insert(hashValue: Long): void
        check(hashValue: Long): boolean
    }
}
