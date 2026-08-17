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

import lombok.Getter;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.polypheny.db.adapter.parquet.shared.schema.ParquetNameNormalizer.uniquifyParquetFieldNames;

/**
 * Contains inferred schema: all fields definitions
 */
@Getter
public class SchemaState {

    public static final String CONFLICT_STRINGIFY = "stringify";
    public static final String CONFLICT_FAIL = "fail";

    private final List<FieldSchema> fields = new ArrayList<>();
    // defines what to do when sampled values for the same field are incompatible during schema inference
    // defined in activity settings
    private final String conflictMode;


    public SchemaState( String conflictMode ) {
        this.conflictMode = conflictMode;
    }


    /**
     * creates the initial schema model for relational export before any sample rows are inspected
     *
     * @param inputType - relational input type
     * @param keepPk - boolean
     */
    public void init( AlgDataType inputType, boolean keepPk ) {
        List<AlgDataTypeField> fields = inputType.getFields();
        int start = keepPk ? 0 : 1;
        for ( int i = start; i < fields.size(); i++ ) {
            AlgDataTypeField field = fields.get( i );
            String sourceName = field.getName();
            // create an entry for each exported column
            ValueSchema valueSchema = fromPolyType( field.getType().getPolyType() );
            if ( valueSchema == null ) {
                valueSchema = ValueSchema.stringType();
            }
            this.fields.add( new FieldSchema( sourceName, sourceName, i, false, valueSchema ) );
        }
        // normalize and uniquify names of parquet file columns
        uniquifyParquetFieldNames( this.fields );
    }


    public void addField( FieldSchema field ) {
        this.fields.add( field );
    }


    /**
     * Takes a sampled relational row and merges each exported column’s actual value into the current inferred Parquet schema
     *
     * @param row - input data
     * @param inputType - contains exported columns
     * @param keepPk - boolean
     */
    public void mergeRelationalRowSchema( List<PolyValue> row, AlgDataType inputType, boolean keepPk ) {
        // read fields
        List<AlgDataTypeField> fields = inputType.getFields();
        int start = keepPk ? 0 : 1;
        int schemaIndex = 0;
        // iterate over exported columns of sampled row
        for ( int i = start; i < fields.size(); i++ ) {
            PolyValue value = row.get( i );
            FieldSchema schema = this.fields.get( schemaIndex++ );
            ValueSchema inferredValueSchema = ValueSchema.inferSchema( value, conflictMode );
            if ( inferredValueSchema == null ) {
                continue;
            }
            // if the value is not null, merge it into the existing field schema
            ValueSchema mergedValueSchema = merge( schema.getValueSchema(), inferredValueSchema );
            schema.setValueSchema( mergedValueSchema );
        }
    }


    /**
     * Updates the inferred Parquet schema using one sampled input document
     *
     * @param document - input row
     * @param keepId - boolean
     */
    public void mergeDocumentSchema( PolyDocument document, boolean keepId ) {
        // iterate through all document fields
        for ( Map.Entry<PolyString, PolyValue> entry : document.entrySet() ) {
            String sourceName = entry.getKey().value;
            if ( !keepId && DocumentType.DOCUMENT_ID.equals( sourceName ) ) {
                continue; // skip id
            }
            // infer schema from value
            PolyValue value = entry.getValue();
            ValueSchema inferred = ValueSchema.inferSchema( value, conflictMode );
            if ( inferred == null ) {
                continue;
            }
            // merge information into running schema state
            mergeFieldValueSchema( sourceName, inferred );
        }
        // normalize and uniquify names of parquet file columns
        uniquifyParquetFieldNames( this.fields );
    }


    private void mergeFieldValueSchema( String sourceName, ValueSchema valueSchema ) {
        for ( FieldSchema field : fields ) {
            if ( field.getSourceName().equals( sourceName ) ) {
                field.setValueSchema( merge( field.getValueSchema(), valueSchema ) );
                return;
            }
        }
        fields.add( new FieldSchema( sourceName, sourceName, -1, false, valueSchema ) );
    }


    private ValueSchema merge( ValueSchema left, ValueSchema right ) {
        return left.mergeValueSchemas( right, conflictMode );
    }


    private static ValueSchema fromPolyType( PolyType type ) {
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
            case ARRAY -> ValueSchema.repeated( ValueSchema.stringType() );
            case DOCUMENT -> ValueSchema.groupType();
            default -> ValueSchema.stringType();
        };
    }


}
