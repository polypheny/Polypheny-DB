/*
 * Copyright 2019-2026 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.adapter.parquet.shared.schema.inference;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.polypheny.db.adapter.parquet.shared.schema.ParquetNameNormalizer.uniquifyParquetFieldNames;


/**
 * Describes value the parquet field contains:
 * Represent Parquet-compatible value shapes during inference and writing,
 * before they are converted into actual Parquet Type objects
 *
 * @param kind - logical category of values
 * @param primitiveTypeName - primitive type to use when it is primitive value
 * @param logicalType - Parquet logical annotation
 * @param nested - used when the value is a nested object - List<FieldSchema>
 * @param repeated - whether the value is a repeated/list value
 * @param elementSchema - if repeated = true, this describes the schema of each list element
 */
public record ValueSchema( ValueKind kind, PrimitiveType.PrimitiveTypeName primitiveTypeName, LogicalTypeAnnotation logicalType, List<FieldSchema> nested, boolean repeated, ValueSchema elementSchema ) {

    public ValueSchema( ValueKind kind, PrimitiveType.PrimitiveTypeName primitiveTypeName, LogicalTypeAnnotation logicalType, List<FieldSchema> nested, boolean repeated, ValueSchema elementSchema ) {
        this.kind = kind;
        this.primitiveTypeName = primitiveTypeName;
        this.logicalType = logicalType;
        this.nested = nested == null ? new ArrayList<>() : nested;
        this.repeated = repeated;
        this.elementSchema = elementSchema;
    }


    //region factory methods
    public static ValueSchema booleanType() {
        return new ValueSchema( ValueKind.BOOLEAN, PrimitiveType.PrimitiveTypeName.BOOLEAN, null, null, false, null );
    }


    public static ValueSchema int32Type() {
        return new ValueSchema( ValueKind.INT32, PrimitiveType.PrimitiveTypeName.INT32, null, null, false, null );
    }


    public static ValueSchema int64Type() {
        return new ValueSchema( ValueKind.INT64, PrimitiveType.PrimitiveTypeName.INT64, null, null, false, null );
    }


    public static ValueSchema floatType() {
        return new ValueSchema( ValueKind.FLOAT, PrimitiveType.PrimitiveTypeName.FLOAT, null, null, false, null );
    }


    public static ValueSchema doubleType() {
        return new ValueSchema( ValueKind.DOUBLE, PrimitiveType.PrimitiveTypeName.DOUBLE, null, null, false, null );
    }


    public static ValueSchema stringType() {
        return new ValueSchema( ValueKind.STRING, PrimitiveType.PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType(), null, false, null );
    }


    public static ValueSchema binaryType() {
        return new ValueSchema( ValueKind.BINARY, PrimitiveType.PrimitiveTypeName.BINARY, null, null, false, null );
    }


    public static ValueSchema dateType() {
        return new ValueSchema( ValueKind.DATE, PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.dateType(), null, false, null );
    }


    public static ValueSchema timeType() {
        return new ValueSchema( ValueKind.TIME, PrimitiveType.PrimitiveTypeName.INT32, LogicalTypeAnnotation.timeType( true, LogicalTypeAnnotation.TimeUnit.MILLIS ), null, false, null );
    }


    public static ValueSchema timestampType() {
        return new ValueSchema( ValueKind.TIMESTAMP, PrimitiveType.PrimitiveTypeName.INT64, LogicalTypeAnnotation.timestampType( true, LogicalTypeAnnotation.TimeUnit.MILLIS ), null, false, null );
    }


    public static ValueSchema groupType() {
        return new ValueSchema( ValueKind.GROUP, null, null, new ArrayList<>(), false, null );
    }


    public static ValueSchema repeated( ValueSchema elementSchema ) {
        return new ValueSchema( elementSchema.kind, elementSchema.primitiveTypeName, elementSchema.logicalType, copyNested( elementSchema.nested ), true, elementSchema.copyNonRepeated() );
    }
    //endregion


    /**
     * Infer field schema from input value
     *
     * @param value - input PolyValue
     * @param conflictMode - defines what to do when sampled values for the same field are incompatible during schema inference
     * @return ValueSchema
     */
    public static ValueSchema inferSchema( PolyValue value, String conflictMode ) {
        if ( value == null || value.isNull() ) {
            return null;
        }
        if ( value.isList() ) {
            return inferListSchema( value.asList(), conflictMode );
        }
        if ( value.isDocument() ) {
            return inferDocumentSchema( value.asDocument(), conflictMode );
        }
        if ( value.isMap() ) {
            return ValueSchema.stringType();
        }
        return inferPrimitiveSchema( value );
    }


    /**
     * Called when the current value schema is a nested group/document
     * look for a child field with a given name inside group
     * if it already exists, merge the new child schema into the existing one
     * if it does not exist yet, add it as a new child field
     *
     * @param sourceName - input field name
     * @param valueSchema - schema to infer
     * @param conflictMode - conflict mode
     */
    public void mergeNested( String sourceName, ValueSchema valueSchema, String conflictMode ) {
        for ( FieldSchema child : nested ) {
            if ( child.getSourceName().equals( sourceName ) ) {
                child.setValueSchema( child.getValueSchema().mergeValueSchemas( valueSchema, conflictMode ) );
                return;
            }
        }
        nested.add( new FieldSchema( sourceName, sourceName, -1, false, valueSchema ) );
    }


    /**
     * Combines two inferred schemas for the same field into one schema
     *
     * @param valueSchema - schema to merge with
     * @param conflictMode - conflict mode
     * @return ValueSchema
     */
    public ValueSchema mergeValueSchemas( ValueSchema valueSchema, String conflictMode ) {
        if ( this.repeated != valueSchema.repeated ) {
            return resolveConflict( conflictMode );
        }

        if ( this.repeated ) {
            ValueSchema mergedElement = elementSchema.mergeValueSchemas( valueSchema.elementSchema, conflictMode );
            return ValueSchema.repeated( mergedElement );
        }

        if ( this.kind == valueSchema.kind ) {
            if ( kind == ValueKind.GROUP ) {
                ValueSchema merged = ValueSchema.groupType();
                Map<String, FieldSchema> mergedFields = new LinkedHashMap<>();
                for ( FieldSchema field : nested ) {
                    mergedFields.put( field.getSourceName(), field.copy() );
                }
                for ( FieldSchema field : valueSchema.nested ) {
                    FieldSchema existing = mergedFields.get( field.getSourceName() );
                    if ( existing == null ) {
                        mergedFields.put( field.getSourceName(), field.copy() );
                    } else {
                        existing.setValueSchema( existing.getValueSchema().mergeValueSchemas( field.getValueSchema(), conflictMode ) );
                    }
                }
                merged.nested.addAll( mergedFields.values() );
                uniquifyParquetFieldNames( merged.nested );
                return merged;
            }
            return this;
        }

        if ( isNumeric( this.kind ) && isNumeric( valueSchema.kind ) ) {
            return widestNumeric( this.kind, valueSchema.kind );
        }

        return resolveConflict( conflictMode );
    }


    public ValueSchema copy() {
        if ( repeated ) {
            return repeated( elementSchema.copyNonRepeated() );
        }
        return copyNonRepeated();
    }


    private static List<FieldSchema> copyNested( List<FieldSchema> nested ) {
        List<FieldSchema> items = new ArrayList<>( nested.size() );
        for ( FieldSchema item : nested ) {
            items.add( item.copy() );
        }
        return items;
    }


    private ValueSchema copyNonRepeated() {
        return new ValueSchema( kind, primitiveTypeName, logicalType, copyNested( nested ), false, null );
    }


    private static ValueSchema resolveConflict( String conflictMode ) {
        if ( SchemaState.CONFLICT_STRINGIFY.equals( conflictMode ) ) {
            return ValueSchema.stringType();
        }
        throw new GenericRuntimeException( "Incompatible values encountered while inferring Parquet schema." );
    }


    private static boolean isNumeric( ValueKind kind ) {
        return kind == ValueKind.INT32 || kind == ValueKind.INT64 || kind == ValueKind.FLOAT || kind == ValueKind.DOUBLE;
    }


    private static ValueSchema widestNumeric( ValueKind left, ValueKind right ) {
        if ( left == ValueKind.DOUBLE || right == ValueKind.DOUBLE ) {
            return ValueSchema.doubleType();
        }
        if ( left == ValueKind.FLOAT || right == ValueKind.FLOAT ) {
            return ValueSchema.floatType();
        }
        if ( left == ValueKind.INT64 || right == ValueKind.INT64 ) {
            return ValueSchema.int64Type();
        }
        return ValueSchema.int32Type();
    }


    private static ValueSchema inferDocumentSchema( PolyDocument document, String conflictMode ) {
        ValueSchema group = ValueSchema.groupType();
        for ( Map.Entry<PolyString, PolyValue> entry : document.entrySet() ) {
            ValueSchema fieldSchema = inferSchema( entry.getValue(), conflictMode );
            if ( fieldSchema == null ) {
                continue;
            }
            group.mergeNested( entry.getKey().value, fieldSchema, conflictMode );
        }
        uniquifyParquetFieldNames( group.nested() );
        return group.nested().isEmpty() ? null : group;
    }


    private static ValueSchema inferListSchema( PolyList<PolyValue> list, String conflictMode ) {
        ValueSchema element = null;
        for ( PolyValue item : list ) {
            ValueSchema itemSchema = inferSchema( item, conflictMode );
            if ( itemSchema == null ) {
                continue;
            }
            element = element == null ? itemSchema : element.mergeValueSchemas( itemSchema, conflictMode );
        }
        if ( element == null ) {
            element = ValueSchema.stringType();
        }
        return ValueSchema.repeated( element );
    }


    private static ValueSchema inferPrimitiveSchema( PolyValue value ) {
        PolyType type = value.getType();
        return switch ( type ) {
            case BOOLEAN -> ValueSchema.booleanType();
            case TINYINT, SMALLINT, INTEGER -> ValueSchema.int32Type();
            case BIGINT -> ValueSchema.int64Type();
            case FLOAT, REAL -> ValueSchema.floatType();
            case DOUBLE, DECIMAL -> ValueSchema.doubleType();
            case DATE -> ValueSchema.dateType();
            case TIME -> ValueSchema.timeType();
            case TIMESTAMP -> ValueSchema.timestampType();
            case VARBINARY, BINARY -> ValueSchema.binaryType();
            default -> ValueSchema.stringType();
        };
    }

}
