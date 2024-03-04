import { writeFile } from 'fs/promises'

import {
    EmissionFactor,
    EmissionFactorDenominatorUnit,
    EmissionFactorSource,
    LuneClient,
    Mass,
} from '@lune-climate/lune'
import { program } from 'commander'
import { stringify } from 'csv-stringify/sync'
import _ from 'lodash'
import { Err, Ok, Result } from 'ts-results-es'

import { isRunningAsScript, loadCsv as loadCsvUtils } from 'src/utils.js'

program
    .name('lune-ef-mapping-tool')
    .option('-o, --output <output-csv-file>', 'The CSV file storing the results')
    .option(
        '-n, --name <name-column>',
        'The name of the column containing the label/name/classification',
    )
    .option('-r, --region <region-column>', 'The name of the column containing the region')
    .option(
        '-a, --activity-value <activity-value-column>',
        'Activity value column. Activity value and unit are both required or none. Performs an emission calculations',
    )
    .option(
        '-u, --activity-unit <activity-unit-column>',
        'Activity unit column. Activity value and unit are both required or none. Performs an emission calculations',
    )
    .option('-v, --verbose', 'Verbose')
    .argument('<csv-file>', 'The CSV file to use as source')

type CsvDataIn = {
    name: string
    region?: string
    activity?: {
        value: string
        unit: string
    }
}

type CsvDataOut = Record<string, string> & {
    emission_factors: string
    emissions?: string
    error: string
}

let verbose: boolean = false
function log<T>(o: T) {
    if (verbose) {
        console.error(o)
    }
}

async function loadCsv(
    filename: string,
    cols?: {
        name?: string
        region?: string
        activity?: {
            value: string
            unit: string
        }
    },
): Promise<Result<{ row: Record<string, string>; data: CsvDataIn }[], string>> {
    const name = cols?.name ?? 'name'
    const region = cols?.region
    const activity = cols?.activity

    const fields = [name]
    if (region) {
        fields.push(region)
    }
    if (region) {
        fields.push(region)
    }
    if (activity) {
        fields.push(activity.value)
        fields.push(activity.unit)
    }
    const source = await loadCsvUtils(filename, fields)
    if (source.isErr()) {
        return source
    }

    const rows: { row: Record<string, string>; data: CsvDataIn }[] = []
    for (let i = 0; i < source.value.length; i++) {
        const row = source.value[i]

        if (row[name] === '') {
            return Err(`row ${i + 1}: ${name} must not be blank`)
        }

        rows.push({
            row,
            data: {
                name: row[name].trim(),
                ...(region ? { region: row[region] === '' ? undefined : row[region].trim() } : {}),
                ...(activity
                    ? {
                          activity: {
                              value: row[activity.value].trim(),
                              unit: row[activity.unit].trim(),
                          },
                      }
                    : {}),
            },
        })
    }

    return Ok(rows)
}

async function resolveClientData(
    luneClient: LuneClient,
    data: CsvDataIn,
): Promise<Result<EmissionFactor[], string>> {
    const result = await luneClient.listEmissionFactors({
        name: data.name,
        limit: '10',
        source: [
            EmissionFactorSource.EXIOBASE,
            ...(data.region?.toLowerCase() === 'united states of america'
                ? [EmissionFactorSource.EPA]
                : []),
        ],
        publicationYear: [2021, 2022],
        ...(data.region ? { region: [data.region] } : {}),
    })

    if (result.isErr()) {
        return Err(result.error.description)
    }

    return Ok(result.value.data)
}

async function calculateEmissions(
    luneClient: LuneClient,
    emissionFactorId: string,
    amount: string,
    currency: string,
): Promise<Result<Mass, string>> {
    const result = await luneClient.createEmissionFactorEstimate({
        emissionFactorId,
        activity: {
            value: amount,
            unit: currency as EmissionFactorDenominatorUnit,
        },
    })

    if (result.isErr()) {
        return Err(result.error.description)
    }

    return Ok(result.value.mass)
}

// eslint-disable-next-line complexity
async function main(): Promise<void> {
    program.parse(process.argv)
    if (program.args.length < 1) {
        program.help()
    }

    if (!process.env.API_KEY) {
        console.error('API_KEY environment variable is required but has not been set')
        process.exit(1)
    }

    const apiKey = process.env.API_KEY
    const luneClient = new LuneClient(apiKey)
    const output = program.opts().output
    const nameColumn = program.opts().name
    const regionColumn = program.opts().region
    const activityValueColumn = program.opts().activityValue
    const activityUnitColumn = program.opts().activityUnit
    verbose = program.opts().verbose

    if (!activityValueColumn || !activityUnitColumn) {
        console.error('activity-value and activity-unit are both required or none is required')
        process.exit(1)
    }

    const filename = program.args[0]
    const rows = await loadCsv(filename, {
        name: nameColumn,
        region: regionColumn,
        activity: activityValueColumn
            ? { value: activityValueColumn, unit: activityUnitColumn }
            : undefined,
    })
    if (rows.isErr()) {
        console.error(rows.error)
        process.exit(1)
    }

    const out: CsvDataOut[] = []
    for (let i = 0; i < rows.value.length; i++) {
        const row = rows.value[i]
        const { row: sourceRow, data } = row
        const result = await resolveClientData(luneClient, data)
        if (result.isErr()) {
            out.push({
                ...sourceRow,
                emission_factors: '',
                emissions: '',
                error: result.error,
            })
            log(
                `❌ Row ${i}: ${data.name}, ${data.region ?? ''} : failed to cmap to emission factors: ${result.error}`,
            )
            continue
        }

        const uniqueEmissionFactors = _.uniq(result.value.map(({ id, name }) => ({ id, name })))

        const emissions =
            data.activity && uniqueEmissionFactors.length
                ? await calculateEmissions(
                      luneClient,
                      uniqueEmissionFactors[0].id,
                      data.activity.value,
                      data.activity.unit,
                  )
                : Ok(undefined)

        if (emissions.isErr()) {
            out.push({
                ...sourceRow,
                emission_factors: '',
                emissions: '',
                error: emissions.error,
            })
            log(
                `❌ Row ${i}: ${data.name}, ${data.region ?? ''} : failed to calculate emissions: ${emissions.error}`,
            )
            continue
        }

        const emissionsTCO2 = emissions.value ? emissions.value.amount : undefined
        out.push({
            ...sourceRow,
            emission_factors: uniqueEmissionFactors.map(({ name }) => name).join('; '),
            ...(emissionsTCO2 ? { emissions: emissionsTCO2 } : {}),
            error: '',
        })

        if (uniqueEmissionFactors.length === 0) {
            log(
                `⚠️  Row ${i}: ${data.name}, ${data.region ?? ''}, could not map to an emission factor`,
            )
        } else {
            log(
                `✅ Row ${i}: ${data.name}, ${data.region ?? ''} -> ${uniqueEmissionFactors[0]?.name}, emissions: ${emissionsTCO2}`,
            )
        }
    }

    const csvText = stringify(out, { header: true, quoted: true })

    if (output) {
        await writeFile(output, csvText)
    } else {
        console.log(csvText)
    }
}

if (isRunningAsScript(import.meta.url)) {
    await main()
}
