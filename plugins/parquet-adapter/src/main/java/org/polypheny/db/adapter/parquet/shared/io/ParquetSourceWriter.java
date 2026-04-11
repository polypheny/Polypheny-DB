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

package org.polypheny.db.adapter.parquet.shared.io;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.polypheny.db.adapter.parquet.shared.schema.ParquetTypeConverter;
import org.polypheny.db.adapter.parquet.shared.schema.inference.FieldSchema;
import org.polypheny.db.adapter.parquet.shared.schema.inference.SchemaState;
import org.polypheny.db.adapter.parquet.shared.schema.inference.ValueKind;
import org.polypheny.db.adapter.parquet.shared.schema.inference.ValueSchema;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;

public class ParquetSourceWriter implements AutoCloseable {

    public static final String COMPRESSION_SNAPPY = "snappy";
    public static final String COMPRESSION_GZIP = "gzip";
    public static final String COMPRESSION_UNCOMPRESSED = "uncompressed";

    private static final ParquetTypeConverter parquetTypeConverter = new ParquetTypeConverter();

    private final ParquetWriter<Group> writer;
    private final SimpleGroupFactory groupFactory;
    private final SchemaState schemaState;
    private final boolean keepId;


    /**
     * Create Parquet writer
     *
     * @param file - file to write
     * @param schema - parquet type schema
     * @param compression - compression type setting
     */
    public ParquetSourceWriter( File file, MessageType schema, String compression, SchemaState schemaState, boolean keepId ) {
        try {
            this.schemaState = schemaState;
            this.keepId = keepId;
            this.groupFactory = new SimpleGroupFactory( schema );
            this.writer = ExampleParquetWriter.builder( new Path( file.toURI() ) )
                    .withType( schema )
                    .withCompressionCodec( getCompressionCodec( compression ) )
                    .build();
        } catch ( Exception e ) {
            throw new GenericRuntimeException( "Unable to open parquet file for writing: " + file.getAbsolutePath(), e );
        }
    }


    @FunctionalInterface
    public interface ProgressListener {

        void rowWritten( long written );

    }


    /**
     * Write rows to parquet file
     */
    public <T> void writeRows( Iterator<T> rows, ProgressListener progressListener ) {
        long written = 0;

        while ( rows.hasNext() ) {
            try {
                var row = rows.next();

                // creates a new empty Parquet row object according to the given Parquet schema
                Group group = groupFactory.newGroup();
                if ( row instanceof PolyDocument document ) {
                    populateGroupWithDocument( group, schemaState.getFields(), document, keepId );
                } else {
                    //noinspection unchecked
                    populateGroupWithRelationalRow( group, schemaState.getFields(), (List<PolyValue>) row );
                }

                writer.write( group );
                written++;

                if ( progressListener != null ) {
                    progressListener.rowWritten( written );
                }
            } catch ( Exception e ) {
                throw new GenericRuntimeException( "Error while writing parquet data", e );
            }
        }
    }


    /**
     * Get parquet compression format according to chosen settings
     *
     * @param compression - settings
     * @return CompressionCodecName
     */
    private static CompressionCodecName getCompressionCodec( String compression ) {
        return switch ( compression ) {
            case COMPRESSION_SNAPPY -> CompressionCodecName.SNAPPY;
            case COMPRESSION_GZIP -> CompressionCodecName.GZIP;
            case COMPRESSION_UNCOMPRESSED -> CompressionCodecName.UNCOMPRESSED;
            default -> throw new IllegalArgumentException( "Unknown compression: " + compression );
        };
    }


    /**
     * Close writer
     *
     * @throws IOException - exception
     */
    @Override
    public void close() throws IOException {
        writer.close();
    }


    /**
     * write row
     *
     * @param group - empty parquet row with schema columns
     * @param fields - columns
     * @param row - input data
     */
    private static void populateGroupWithRelationalRow( Group group, List<FieldSchema> fields, List<PolyValue> row ) {
        for ( FieldSchema field : fields ) {
            populateGroupWithValue( group, field, row.get( field.getSourceIndex() ) );
        }
    }


    private static void populateGroupWithSingleValue( Group group, int fieldIndex, ValueSchema schema, PolyValue value ) {
        if ( schema.kind() == ValueKind.GROUP ) {
            if ( !value.isDocument() ) {
                throw new GenericRuntimeException( "Expected a document value for nested Parquet group field." );
            }
            Group child = group.addGroup( fieldIndex );
            populateGroupWithDocument( child, schema.nested(), value.asDocument(), true );
            return;
        }
        addPrimitiveValue( group, fieldIndex, schema, value );
    }


    private static void populateGroupWithRepeatedValue( Group group, int fieldIndex, ValueSchema schema, PolyValue value ) {
        if ( value == null || value.isNull() ) {
            return;
        }
        if ( !value.isList() ) {
            if ( schema.kind() == ValueKind.GROUP ) {
                throw new GenericRuntimeException( "Expected a list for repeated group field." );
            }
            addPrimitiveValue( group, fieldIndex, schema, parquetTypeConverter.valueToPolyString( value ) );
            return;
        }

        for ( PolyValue item : value.asList() ) {
            if ( item == null || item.isNull() ) {
                continue;
            }
            if ( schema.kind() == ValueKind.GROUP ) {
                if ( !item.isDocument() ) {
                    throw new GenericRuntimeException( "Expected a document inside repeated group field." );
                }
                Group child = group.addGroup( fieldIndex );
                populateGroupWithDocument( child, schema.nested(), item.asDocument(), true );
            } else {
                addPrimitiveValue( group, fieldIndex, schema, item );
            }
        }
    }


    private static void populateGroupWithDocument( Group group, List<FieldSchema> fields, PolyDocument document, boolean keepId ) {
        for ( FieldSchema field : fields ) {
            if ( !keepId && DocumentType.DOCUMENT_ID.equals( field.getSourceName() ) ) {
                continue;
            }
            populateGroupWithValue( group, field, document.get( PolyString.of( field.getSourceName() ) ) );
        }
    }


    private static void populateGroupWithValue( Group group, FieldSchema field, PolyValue value ) {
        if ( value == null || value.isNull() ) {
            return;
        }

        int fieldIndex = group.getType().getFieldIndex( field.getParquetName() );
        if ( field.getValueSchema().repeated() ) {
            if ( !value.isList() ) {
                value = parquetTypeConverter.valueToPolyString( value );
            }
            populateGroupWithRepeatedValue( group, fieldIndex, field.getValueSchema().elementSchema(), value );
            return;
        }
        populateGroupWithSingleValue( group, fieldIndex, field.getValueSchema(), value );
    }


    /**
     * Write one scalar (primitive) value into a Parquet Group field
     *
     * @param group - row to write
     * @param fieldIndex - column to write
     * @param schema - ValueSchema
     * @param value - PolyValue to write
     */
    private static void addPrimitiveValue( Group group, int fieldIndex, ValueSchema schema, PolyValue value ) {
        if ( value == null || value.isNull() ) {
            return;
        }

        PrimitiveType type = (PrimitiveType) group.getType().getType( fieldIndex );
        PolyValue valueToWrite = schema.kind() == ValueKind.STRING ? parquetTypeConverter.valueToPolyString( value ) : value;
        Object parquetValue = parquetTypeConverter.fromPolyValueToParquetObj( type, valueToWrite );
        if ( parquetValue == null ) {
            throw new GenericRuntimeException( "Value '" + value + "' is incompatible with the inferred Parquet schema." );
        }

        switch ( schema.primitiveTypeName() ) {
            case BOOLEAN -> group.add( fieldIndex, (boolean) parquetValue );
            case INT32 -> group.add( fieldIndex, (int) parquetValue );
            case INT64 -> group.add( fieldIndex, (long) parquetValue );
            case FLOAT -> group.add( fieldIndex, (float) parquetValue );
            case DOUBLE -> group.add( fieldIndex, (double) parquetValue );
            case BINARY, FIXED_LEN_BYTE_ARRAY, INT96 -> group.add( fieldIndex, (Binary) parquetValue );
            default -> throw new GenericRuntimeException( "Unsupported Parquet primitive type: " + schema.primitiveTypeName() );
        }
    }

}
