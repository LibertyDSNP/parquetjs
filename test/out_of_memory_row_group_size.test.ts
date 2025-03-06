import {AnnouncementType, parquet, descriptorForAnnouncementType} from "@dsnp/schemas";
import {assert, expect} from 'chai';
import {ParquetSchema, ParquetWriter} from "../parquet";
import path from "path";
import {createReadStream, promises} from "node:fs";
import split from 'split2';
import * as wkx from "wkx";

// Log the memory usage every 5 seconds
/*
setInterval(() => {
  const memoryUsage = process.memoryUsage();
  console.log(memoryUsage);
}, 5000);
*/

const schema = new ParquetSchema({
  source_name: {type: 'UTF8'},
  geometry: {type: 'BYTE_ARRAY', optional: true},
  id: {type: 'UTF8', optional: true},
  pid: {type: 'UTF8', optional: true},
  number: {type: 'UTF8', optional: true},
  street: {type: 'UTF8', optional: true},
  unit: {type: 'UTF8', optional: true},
  city: {type: 'UTF8', optional: true},
  postcode: {type: 'UTF8', optional: true},
  district: {type: 'UTF8', optional: true},
  region: {type: 'UTF8', optional: true},
  addrtype: {type: 'UTF8', optional: true},
  notes: {type: 'UTF8', optional: true}
});

import { promises as fs } from 'fs';
import path from 'path';

async function findGeoJSONFiles(rootDir: string, maxFiles: number = 1): Promise<string[]> {
  let geojsonFiles: string[] = [];

  async function traverseDirectory(dir: string) {
    try {
      const entries = await fs.readdir(dir, { withFileTypes: true });

      for (const entry of entries) {

        const fullPath = path.join(dir, entry.name);

        if (entry.isDirectory()) {
          await traverseDirectory(fullPath); // Recursively search subdirectories
        } else if (entry.isFile() && entry.name.endsWith('.geojson')) {
          geojsonFiles.push(fullPath);
          if (entry === entries[maxFiles-1]) {
            console.log(`hit the limit of ${maxFiles} files`)
            return geojsonFiles;
          }
        }
      }
    } catch (error) {
      console.error(`Error reading directory ${dir}:`, error);
    }
  }

  await traverseDirectory(rootDir);
  return geojsonFiles;
}


describe("out of memory 4096 default", () => {
  it("uses up too much memory", async () => {
    // tmpdir where input & output files live
    const tmp: string = "/tmp/oom-parquetjs";
    // input file names

    const name: string = "oom-4096-output";
    const files = await findGeoJSONFiles(path.resolve(tmp, 'ca'), 300);
    // const files = [path.resolve(tmp, "ca", "countrywide-addresses-country.geojson")];
    // expect(files.length).to.be.greaterThan(0, "no files found");
    const writer = await ParquetWriter.openFile(schema, path.resolve(tmp, `${name}.parquet`));
    // writer.setRowGroupSize(4096);

    for (const data of files) {
      // const resolved_data_filename = path.resolve(tmp, 'sources', data);
      const resolved_data_filename = data;

      // Read the file and parse it as linefeed-delimited JSON
      const data_stream = createReadStream(resolved_data_filename);
      const data_lines = data_stream.pipe(split());
      let line_count = 0;

      for await (const line of data_lines) {
        line_count++;
        const record = JSON.parse(line);
        const properties = record.properties;

        // GeoParquet expects the geometry as a WKB
        let wkbGeometry = null;
        if (record.geometry && record.geometry.type) {
          wkbGeometry = wkx.Geometry.parseGeoJSON(record.geometry).toWkb();
        } else {
          console.error(`not ok - ${resolved_data_filename} line ${line_count} has no geometry: ${line}`);
          break;
        }

        await writer.appendRow({
          source_name: data,
          geometry: wkbGeometry,
          id: properties.id,
          pid: properties.pid,
          number: properties.number,
          street: properties.street,
          unit: properties.unit,
          city: properties.city,
          postcode: properties.postcode,
          district: properties.district,
          region: properties.region,
          addrtype: properties.addrtype,
          notes: properties.notes
        });
      }

      console.info(`ok - ${resolved_data_filename} processed ${line_count} lines and appended to parquet file`);
    }
    console.log("DONE")
    await writer.close();

  }).timeout(120000);
})

// Set up parquet output file
