import { writeFileSync } from "fs";

import {
  EmissionFactor,
  EmissionFactorSource,
  LuneClient,
} from "@lune-climate/lune";
import { program } from "commander";
import { stringify } from "csv-stringify/sync";
import { Err, Ok, Result } from "ts-results-es";

import { isRunningAsScript, loadCsvSelectedFields } from "src/utils.js";

program
  .name("lune-ef-mapping-tool")
  .option("-o, --output <output-csv-file>", "The CSV file storing the results")
  .argument("<csv-file>", "The CSV file to use as source");

type CsvDataIn = {
  name: string;
  region?: string;
};

type CsvDataOut = CsvDataIn & {
  emission_factors: string;
  error: string;
};

async function loadCsv(filename: string): Promise<Result<CsvDataIn[], string>> {
  const data = await loadCsvSelectedFields(filename, ["name", "region"]);

  if (data.isErr()) {
    return data;
  }

  const rows: CsvDataIn[] = [];
  for (let i = 0; i < data.value.length; i++) {
    const row = data.value[i];

    if (row.name === "") {
      return Err(`row ${i + 1}: name must not be blank`);
    }

    rows.push({
      name: row.name.trim(),
      region: row.region === "" ? undefined : row.region.trim(),
    });
  }

  return Ok(rows);
}

async function resolveClientData(
  luneClient: LuneClient,
  data: CsvDataIn,
): Promise<Result<EmissionFactor[], string>> {
  const result = await luneClient.listEmissionFactors({
    name: data.name,
    limit: "10",
    source: [EmissionFactorSource.EXIOBASE],
    publicationYear: [2021],
    ...(data.region ? { region: [data.region] } : {}),
  });

  if (result.isErr()) {
    return Err(result.error.description);
  }

  return Ok(result.value.data);
}

async function main(): Promise<void> {
  program.parse(process.argv);
  if (program.args.length < 1) {
    program.help();
  }

  if (!process.env.API_KEY) {
    console.error(
      "API_KEY environment variable is required but has not been set",
    );
    process.exit(1);
  }

  const apiKey = process.env.API_KEY;
  const luneClient = new LuneClient(apiKey);
  const output = program.opts().output;

  const filename = program.args[0];
  const rows = await loadCsv(filename);
  if (rows.isErr()) {
    console.error(rows.error);
    process.exit(1);
  }

  const out: CsvDataOut[] = [];
  for (const row of rows.value) {
    const result = await resolveClientData(luneClient, row);
    if (result.isErr()) {
      out.push({
        ...row,
        emission_factors: "",
        error: result.error,
      });
      continue;
    }

    out.push({
      ...row,
      emission_factors: JSON.stringify(result.value),
      error: "",
    });
  }

  const csvText = stringify(out, { header: true, quoted: true });

  if (output) {
    writeFileSync(output, csvText);
  } else {
    console.log(csvText);
  }
}

if (isRunningAsScript(import.meta.url)) {
  await main();
}
