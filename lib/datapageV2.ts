///
/// Modified under the MIT License from
/// https://github.com/hyparam/hyparquet
/// The MIT License (MIT)
//
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import {DataReader} from "./codec/types";
import {DataPageHeaderV2} from "../gen-nodejs/parquet_types";
import {readRleBitPackedHybrid} from "./codec/encoding";
import {getBitWidth} from "./util";

/**
 * @typedef {import("./types.d.ts").DataReader} DataReader
 * @param {DataReader} reader
 * @param {DataPageHeaderV2} daph2 data page header v2
 * @param {number} rLevelMax  the maximum of repetition levels
 * @returns {any[]} repetition levels
 */
export const readRepetitionLevelsV2 = (reader: DataReader,
                                       daph2: DataPageHeaderV2,
                                       rLevelMax: number,
): Array<any> => {
  const values = new Array(daph2.num_values)
  if (!rLevelMax) return values.fill(0);
  const bitWidth = getBitWidth(rLevelMax)
  let disableEnvelope = daph2.definition_levels_byte_length === 0
  readRleBitPackedHybrid(reader, bitWidth, daph2.repetition_levels_byte_length, values, disableEnvelope)
  return values
}

/**
 * @typedef {import("./types.d.ts").DataReader} DataReader
 * @param {DataReader} reader
 * @param {DataPageHeaderV2} daph2 data page header v2
 * @param {number} dLevelMax the maximum of definition levels
 * @returns {number[] | undefined} definition levels
 */
export const readDefinitionLevelsV2 = (reader: DataReader,
                                       daph2: DataPageHeaderV2,
                                       dLevelMax: number,
): Array<number>|undefined => {
  if (dLevelMax) {
    // V2 we know the length
    const values = new Array(daph2.num_values)
    const bitWidth = getBitWidth(dLevelMax)
    const disableEnvelope = true
    readRleBitPackedHybrid(reader, bitWidth, daph2.definition_levels_byte_length, values, disableEnvelope)
    return values
  }
}
