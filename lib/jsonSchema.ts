// Support json-schema.org schema conversion to a Parquet file
import { JSONSchema4 } from 'json-schema';
import { FieldDefinition, SchemaDefinition } from './declare';
import * as fields from './fields';
import { TimeUnit } from '../gen-nodejs/parquet_types';
import { TimeType } from '../gen-nodejs/parquet_types';

type SupportedJSONSchema4 = Omit<
  JSONSchema4,
  '$ref' | 'multipleOf' | 'allOf' | 'anyOf' | 'oneOf' | 'not' | 'additionalItems' | 'enum' | 'extends'
>;

/**
 * Simple check to make sure that `SupportedJSONSchema4` is correct.
 * There are a lot of JSON schema stuff we just don't support for now.
 */
const isJsonSchemaSupported = (js: JSONSchema4): js is SupportedJSONSchema4 => {
  const unsupportedFields = [
    '$ref',
    'multipleOf',
    'allOf',
    'anyOf',
    'oneOf',
    'not',
    'additionalItems',
    'enum',
    'extends',
  ];
  for (const field in unsupportedFields) {
    if (!(js[field] === undefined || js[field] === false)) {
      return false;
    }
  }
  return true;
};

/**
 * Error to capture all the unsupported edge cases
 */
export class UnsupportedJsonSchemaError extends Error {
  constructor(msg: string) {
    const message = `Unsupported JSON schema: ${msg}`;
    super(message);
    this.name = 'UnsupportedJsonSchemaError';
  }
}

/**
 * Json Schema has required at the top level instead of field level
 */
const isJsonSchemaRequired =
  (jsonSchema: SupportedJSONSchema4) =>
  (field: string): boolean => {
    switch (jsonSchema.required) {
      case true:
        return true;
      case undefined:
      case false:
        return false;
    }

    return jsonSchema.required.includes(field);
  };

/**
 * Converts the Array field type into the correct Field Definition
 */
const fromJsonSchemaArray = (fieldValue: SupportedJSONSchema4, optionalFieldList: boolean): FieldDefinition => {
  if (!fieldValue.items || !fieldValue.items.type) {
    throw new UnsupportedJsonSchemaError('Array field with no values found.');
  }

  switch (fieldValue.items.type) {
    case 'string':
      if (fieldValue.items.format && fieldValue.items.format === 'date-time') {
        return fields.createListField('TIMESTAMP_MILLIS', optionalFieldList);
      }
      return fields.createListField('UTF8', optionalFieldList);

    case 'integer':
      return fields.createListField('INT64', optionalFieldList);

    case 'number':
      return fields.createListField('DOUBLE', optionalFieldList);

    case 'boolean':
      return fields.createListField('BOOLEAN', optionalFieldList);

    case 'object':
      // Handle array of time fields
      if (
        fieldValue.items.properties &&
        fieldValue.items.properties.unit &&
        fieldValue.items.properties.isAdjustedToUTC
      ) {
        if (!fieldValue.items.properties.unit.enum) {
          throw new UnsupportedJsonSchemaError('Unit enum is not defined');
        }
        const unit = fieldValue.items.properties.unit.default || fieldValue.items.properties.unit.enum[0];
        const isAdjustedToUTC = !!fieldValue.items.properties.isAdjustedToUTC.default;
        let timeUnit: TimeUnit;

        switch (unit) {
          case 'MICROS':
            timeUnit = new TimeUnit({ MICROS: true });
            break;
          case 'NANOS':
            timeUnit = new TimeUnit({ NANOS: true });
            break;
          default:
            timeUnit = new TimeUnit({ MILLIS: true });
            break;
        }

        const timeLogicalType = new TimeType({ isAdjustedToUTC, unit: timeUnit });
        return fields.createTimeField(timeLogicalType, optionalFieldList);
      }

      return fields.createStructListField(fromJsonSchema(fieldValue.items), optionalFieldList);

    default:
      throw new UnsupportedJsonSchemaError(`Array field type ${JSON.stringify(fieldValue.items)} is unsupported.`);
  }
};

/**
 * Converts a field from a JSON Schema into a Parquet Field Definition
 */
const fromJsonSchemaField =
  (jsonSchema: JSONSchema4) =>
  (fieldName: string, fieldValue: JSONSchema4): FieldDefinition => {
    if (!isJsonSchemaSupported(fieldValue)) {
      throw new UnsupportedJsonSchemaError(`Field: ${fieldName} has an unsupported schema`);
    }
    const optional = !isJsonSchemaRequired(jsonSchema)(fieldName);

    switch (fieldValue.type) {
      case 'string':
        if (fieldValue.format && fieldValue.format === 'date-time') {
          return fields.createTimestampField(optional);
        }
        return fields.createStringField(optional);

      case 'integer':
        return fields.createIntField(64, optional);

      case 'number':
        return fields.createDoubleField(optional);

      case 'boolean':
        return fields.createBooleanField(optional);

      case 'array':
        return fromJsonSchemaArray(fieldValue, optional);

      case 'object':
        if (fieldValue.properties && fieldValue.properties.unit && fieldValue.properties.isAdjustedToUTC) {
          if (!fieldValue.properties.unit.enum) {
            throw new UnsupportedJsonSchemaError('Unit enum is not defined');
          }
          const unit = fieldValue.properties.unit.default || fieldValue.properties.unit.enum[0];
          const isAdjustedToUTC = !!fieldValue.properties.isAdjustedToUTC.default;
          let timeUnit: TimeUnit;
          switch (unit) {
            case 'MICROS':
              timeUnit = new TimeUnit({ MICROS: true });
              break;
            case 'NANOS':
              timeUnit = new TimeUnit({ NANOS: true });
              break;
            default:
              timeUnit = new TimeUnit({ MILLIS: true });
              break;
          }

          const timeLogicalType = new TimeType({ isAdjustedToUTC, unit: timeUnit });
          return fields.createTimeField(timeLogicalType, optional);
        }

        return fields.createStructField(fromJsonSchema(fieldValue), optional);

      default:
        throw new UnsupportedJsonSchemaError(
          `Unable to convert "${fieldName}" with JSON Schema type "${fieldValue.type}" to a Parquet Schema.`
        );
    }
  };

/**
 * Converts supported Json Schemas into Parquet Schema Definitions
 */
export const fromJsonSchema = (jsonSchema: JSONSchema4): SchemaDefinition => {
  if (!isJsonSchemaSupported(jsonSchema)) {
    throw new UnsupportedJsonSchemaError('Unsupported fields found');
  }

  const schema: SchemaDefinition = {};

  const fromField = fromJsonSchemaField(jsonSchema);

  for (const [fieldName, fieldValue] of Object.entries(jsonSchema.properties || {})) {
    schema[fieldName] = fromField(fieldName, fieldValue);
  }

  return schema;
};
