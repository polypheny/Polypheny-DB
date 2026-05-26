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

package org.polypheny.db.workflow.parquet;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.polypheny.db.adapter.parquet.document.execution.ParquetDocValueExtractor;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetRelValueExtractor;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSchemaReader;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSourceReader;
import org.polypheny.db.adapter.parquet.shared.schema.ParquetNameNormalizer;
import org.polypheny.db.adapter.parquet.shared.schema.ParquetTypeConverter;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeFactory.Builder;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.util.Source;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.ActivityUtils;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;

/**
 * Parquet-reading/conversion helper layer
 */
public final class ParquetWorkflowExtractSupport {

    public static final Set<String> EXTENSIONS = Set.of( "parquet" );
    public static final String OUTPUT_DOCUMENT = "document";
    public static final String OUTPUT_RELATIONAL = "relational";

    private static final AlgDataTypeFactory FACTORY = AlgDataTypeFactory.DEFAULT;
    private static final PolyString FILE_NAME_KEY = PolyString.of( "fileName" );

    private static final ParquetTypeConverter parquetTypeConverter = new ParquetTypeConverter();
    private static final ParquetDocValueExtractor parquetDocValueExtractor = new ParquetDocValueExtractor();
    private static final ParquetRelValueExtractor parquetRelValueExtractor = new ParquetRelValueExtractor();

    private static final int MAX_NAME_LENGTH = 40;
    private static final int TRUNCATED_LENGTH = 37;


    /**
     * Create dynamic name for activity node
     *
     * @param outputModel - relational or document
     * @param sources - parquet files
     * @return String
     * @throws URISyntaxException exception
     */
    public static String getDynamicName( String outputModel, List<Source> sources ) throws URISyntaxException {
        String prefix = OUTPUT_RELATIONAL.equals( outputModel ) ? "Extract Parquet Table" : "Extract Parquet Document";
        if ( sources.size() > 1 ) {
            return prefix + "s";
        }
        String name = ActivityUtils.resourceNameFromSource( sources.get( 0 ) );
        if ( name.length() > MAX_NAME_LENGTH ) {
            name = name.substring( 0, TRUNCATED_LENGTH ) + "...";
        }
        return prefix + ": " + name;
    }


    /**
     * Convert a Parquet file schema into the workflow engine’s output type
     *
     * @param source - parquet file
     * @param outputModel - workflow output type
     * @param addNameField - if add filename as column
     * @return AlgDataType
     */
    public static AlgDataType getOutputType( Source source, String outputModel, boolean addNameField ) {

        if ( OUTPUT_DOCUMENT.equals( outputModel ) ) {
            return DocumentType.ofId();
        }

        MessageType schema = new ParquetSchemaReader( source ).getSchema();

        Builder builder = FACTORY.builder();
        // PK added by the workflow layer, it does not come from file
        builder.add( Activity.PK_COL, null, PolyType.BIGINT );
        for ( Type field : schema.getFields() ) {
            String fieldName = ParquetNameNormalizer.normalizeFieldName( field.getName() );
            builder.add( fieldName, null, getRelationalOutputType( field ) );
        }
        if ( addNameField ) {
            // add source file name to each row
            builder.add( FILE_NAME_KEY.value, null, PolyType.VARCHAR, 255 );
        }
        return builder.uniquify().build();
    }


    /**
     * Read schemas from all selected sources and ensure they are identical for relational output.
     *
     * @param sources selected parquet sources
     */
    public static void validateSharedRelationalSchema( List<Source> sources ) {
        if ( sources.isEmpty() ) {
            throw new GenericRuntimeException( "No parquet files found." );
        }

        MessageType expectedSchema = new ParquetSchemaReader( sources.get( 0 ) ).getSchema();
        for ( int i = 1; i < sources.size(); i++ ) {
            Source source = sources.get( i );
            MessageType currentSchema = new ParquetSchemaReader( source ).getSchema();
            if ( !expectedSchema.equals( currentSchema ) ) {
                throw new GenericRuntimeException(
                        "All parquet sources must share the same schema for relational output. "
                                + "Schema mismatch detected for: " + source.path() );
            }
        }
    }


    /**
     * Call reader functionality to estimate row count
     *
     * @param sources - parquet files
     * @return long - estimation
     */
    public static long estimateTupleCount( List<Source> sources ) {
        long estimate = 0;
        for ( Source source : sources ) {
            try {
                var schemaReader = new ParquetSchemaReader( source );
                estimate += Math.max( schemaReader.getEstimatedRowCount(), 0 );
            } catch ( Exception e ) {
                return -1;
            }
        }
        return estimate == 0 ? -1 : estimate;
    }


    /**
     * Read parquet file and write documents to the output pipe
     * Document created from each row of parquet file
     *
     * @param output - OutputPipe
     * @param source - parquet file
     * @param addNameField - if add filename as column
     * @param maxCount - max number of rows
     * @throws Exception - exception
     */
    public static void writeDocuments( OutputPipe output, Source source, boolean addNameField, int maxCount ) throws Exception {
        String fileName = ActivityUtils.resourceNameFromSource( source );
        PolyString polyName = PolyString.of( fileName );
        try ( ParquetSourceReader reader = new ParquetSourceReader( source ) ) {
            long written = 0;
            for ( Group row = reader.next(); row != null; row = reader.next() ) {
                if ( maxCount >= 0 && written >= maxCount ) {
                    return;
                }
                PolyDocument doc = parquetDocValueExtractor.extractDocument(
                        row,
                        reader.getProjectionSchema(),
                        PolyString.of( fileName + "#" + reader.getCurrentRowNumber() )
                );
                if ( addNameField ) {
                    doc.put( FILE_NAME_KEY, polyName );
                }
                if ( !output.put( doc ) ) {
                    return;
                }
                written++;
            }
        }
    }


    /**
     * Read parquet file and write table to the output pipe
     *
     * @param output - output pipe
     * @param source - parquet file
     * @param addNameCol - if add filename as additional column
     * @param maxCount - rows to read/write
     * @throws Exception - exception
     */
    public static void writeRows( OutputPipe output, Source source, boolean addNameCol, int maxCount ) throws Exception {
        String fileName = ActivityUtils.resourceNameFromSource( source );
        PolyString polyName = PolyString.of( fileName );
        try ( ParquetSourceReader reader = new ParquetSourceReader( source ) ) {
            MessageType schema = reader.getProjectionSchema();
            long written = 0;
            for ( Group row = reader.next(); row != null; row = reader.next() ) {
                if ( maxCount >= 0 && written >= maxCount ) {
                    return;
                }
                List<PolyValue> values = new ArrayList<>( schema.getFieldCount() + (addNameCol ? 2 : 1) );
                values.add( PolyLong.of( reader.getCurrentRowNumber() ) );
                for ( int i = 0; i < schema.getFieldCount(); i++ ) {
                    Type field = schema.getType( i );
                    var polyValue = parquetRelValueExtractor.extractValue( row, i, field );
                    values.add( toRelationalColumnValue( field, polyValue ) );
                }
                if ( addNameCol ) {
                    values.add( polyName );
                }
                if ( !output.put( values ) ) {
                    return;
                }
                written++;
            }
        }
    }


    /**
     * Decides what Polypheny relational column type the workflow should expose for one Parquet field.
     */
    private static PolyType getRelationalOutputType( Type field ) {
        if ( isTextEncodedRelationalField( field ) ) {
            return PolyType.TEXT;
        }
        return parquetTypeConverter.fromParquetTypeToPolyType( field );
    }


    /**
     * Returns:
     * true - for nested/group fields and for repeated fields
     * and should instead be encoded as text/JSON
     * false - for normal scalar primitive fields
     */
    private static boolean isTextEncodedRelationalField( Type field ) {
        return !field.isPrimitive() || field.isRepetition( Type.Repetition.REPEATED );
    }


    /**
     * Prepares one extracted Parquet value so it matches the relational workflow column type.
     */
    private static PolyValue toRelationalColumnValue( Type field, PolyValue value ) {
        if ( value == null || value.isNull() || !isTextEncodedRelationalField( field ) ) {
            return value;
        }
        if ( field.isRepetition( Type.Repetition.REPEATED ) && !value.isList() ) {
            value = PolyList.of( value );
        }
        return ActivityUtils.valueToPolyString( value );
    }

}
