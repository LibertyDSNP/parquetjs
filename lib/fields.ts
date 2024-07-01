// Helper functions for creating fields

import { FieldDefinition, ParquetType, SchemaDefinition } from './declare';

export function createStringField(optional = true, fieldOptions: FieldDefinition = {}): FieldDefinition {
  return { ...fieldOptions, optional, type: 'UTF8' };
}

export function createBooleanField(optional = true, fieldOptions: FieldDefinition = {}): FieldDefinition {
  return { ...fieldOptions, optional, type: 'BOOLEAN' };
}

export function createIntField(size: 32 | 64, optional = true, fieldOptions: FieldDefinition = {}): FieldDefinition {
  return { ...fieldOptions, optional, type: `INT${size}` };
}

export function createFloatField(optional = true, fieldOptions: FieldDefinition = {}): FieldDefinition {
  return { ...fieldOptions, optional, type: 'FLOAT' };
}

export function createDoubleField(optional = true, fieldOptions: FieldDefinition = {}): FieldDefinition {
  return { ...fieldOptions, optional, type: 'DOUBLE' };
}

export function createDecimalField(
  precision: number,
  optional = true,
  fieldOptions: FieldDefinition = {}
): FieldDefinition {
  return { ...fieldOptions, precision, optional, type: 'FLOAT' };
}

export function createTimestampField(optional = true, fieldOptions: FieldDefinition = {}): FieldDefinition {
  return { ...fieldOptions, optional, type: 'TIMESTAMP_MILLIS' };
}

export function createStructField(fields: SchemaDefinition, optional = true): FieldDefinition {
  return {
    optional,
    fields,
  };
}

export function createStructListField(fields: SchemaDefinition, optional = true): FieldDefinition {
  return {
    type: 'LIST',
    optional,
    fields: {
      list: {
        repeated: true,
        fields: {
          element: {
            fields,
          },
        },
      },
    },
  };
}

export function createListField(
  type: ParquetType,
  optional = true,
  elementOptions: FieldDefinition = { optional: true }
): FieldDefinition {
  return {
    type: 'LIST',
    optional,
    fields: {
      list: {
        repeated: true,
        fields: {
          element: {
            optional: true,
            ...elementOptions,
            type,
          },
        },
      },
    },
  };
}
