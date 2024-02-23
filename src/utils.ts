import { readFile } from 'fs/promises'
import { fileURLToPath } from 'url'

import { parse as parseCsv } from 'csv-parse/sync'
import { Err, Ok, Result } from 'ts-results-es'

/**
 * A `Result`-aware variant of `fs/promises.readFile()`.
 *
 * Use this function instead of `readFile()`.
 *
 * All exceptions coming from `readFile()` are converted to `Err()`.
 *
 * @param path The path fo the file to load.
 * @returns `Ok()` with the file contents on success, `Err()` on failure.
 *
 * The `Err()` values aren't currently meant for programmatic consumption, they're
 * useful only when read by humans.
 */
async function readFileChecked(path: string): Promise<Result<Buffer, string>> {
    try {
        return Ok(await readFile(path))
    } catch (e) {
        return Err(`Failed to load ${path}: ${e}`)
    }
}

/**
 * An opinionated, `Result`-aware variant of `csv-parse/sync.parse()`.
 *
 * All exceptions coming from th eoriginal `parse()` are converted to `Err()` values.
 *
 * @param buffer The content to parse as CSV. The first row is expected to contain the
 * column names.
 *
 * @returns `Ok()` with an array of parsed records on success, `Err()` on failure.
 *
 * The `Err()` values aren't currently meant for programmatic consumption, they're
 * useful only when read by humans.
 */
export function parseCsvChecked(buffer: Buffer): Result<object[], string> {
    try {
        return Ok(parseCsv(buffer, { columns: true }))
    } catch (e) {
        return Err(`Failed to parse CSV content: ${e}`)
    }
}

/**
 * Load validated records from a CSV file.
 *
 * Load only the fields we specify and if there are any extra fields
 * they're ignored and discarded.
 *
 * @param path The path to the CSV file.
 * @param fields The fields we expect in the CSV.
 *
 * If not all fields are present the validation will fail.
 *
 * The CSV file is expected to have field names in the first row.
 *
 * @returns `Ok()` with validated content on success, `Err()` in case of failure.
 *
 * The `Err()` contents are intended purely for human consumption.
 */
export async function loadCsvSelectedFields<const T extends readonly string[]>(
    path: string,
    fields: T,
): Promise<Result<Record<T[number], string>[], string>> {
    const rawContent = await readFileChecked(path)
    if (rawContent.isErr()) {
        return rawContent
    }

    function onlySelectedFields(o: unknown): unknown {
        if (typeof o !== 'object' || o === null) {
            return o
        }
        return Object.fromEntries(
            Object.entries(o).filter(([name, _value]) => fields.includes(name)),
        )
    }

    return rawContent
        .andThen(parseCsvChecked)
        .andThen((parsed) =>
            passArray(parsed, (item) => passStringRecord(onlySelectedFields(item), fields)),
        )
}

export function isRunningAsScript(moduleUrl: string): boolean {
    // Use like this: isRunningAsScript(import.meta.url). This will return true if the module
    // is run like this: node path/to/module.js
    return process.argv[1] === fileURLToPath(moduleUrl)
}

/**
 * Verify that the value provided is an array and that it contains elements of the types we expect.
 *
 * @param value The value to validate.
 * @param validator The function to validate each item of the provided array.
 * @returns The properly typed array wrapped in Ok() on success, Err() on failure.
 */
export function passArray<T>(
    value: unknown,
    validator: (item: unknown) => Result<T, string>,
): Result<T[], string> {
    if (!Array.isArray(value)) {
        return Err(`${value} is not an array`)
    }

    if (value.length === 0) {
        return Err('The array is empty')
    }

    const errors = value
        .map((item, index) => validator(item).mapErr((e) => `${index}: ${e}`))
        .filter((result) => result.isErr())
    if (errors.length > 0) {
        return Err(`One or more items failed validation: ${errors.join(', ')}`)
    }

    return Ok(value)
}

/**
 * Return Ok() if passed value is an object with only properties listed in `properties` and
 * all property values are strings, Err() otherwise.
 *
 * Useful for dealing with CSV records for example.
 *
 * @example
 * ```typescript
 *
 * // Pretend this value comes from somewhere and we don't know what's in there
 * const unknownCsvRecord = {'name': 'John', 'age': '20'} as unknown
 *
 * // `parsed` here is of type {name: string; age: string} which allows us to depend on the
 * // shape of the object and to access its properties safely.
 * const parsed = passStringRecord(unknownCsvRecord, ['name', 'age']).unwrap()
 * ```
 */
export function passStringRecord<const T extends readonly string[]>(
    value: unknown,
    properties: T,
): Result<{ [Key in T[number]]: string }, string> {
    if (typeof value !== 'object' || value === null) {
        return Err(`${value} is not an object`)
    }

    for (const p of properties) {
        const propertyValue = p in value ? value[p as keyof typeof value] : undefined
        if (typeof propertyValue !== 'string') {
            return Err(`Property ${p} is not a string: ${typeof propertyValue}`)
        }
    }
    const unwantedProperties = Object.keys(value).filter((p) => !properties.includes(p))
    if (unwantedProperties.length > 0) {
        return Err(`Unwanted properties: ${unwantedProperties}`)
    }
    // SAFETY: This type assertion relies on conditions we verified above.
    return Ok(value as { [Key in T[number]]: string })
}
