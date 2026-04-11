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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.parquet.schema.MessageType;
import org.polypheny.db.adapter.parquet.shared.execution.BufferedIterator;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSourceWriter;
import org.polypheny.db.adapter.parquet.shared.schema.ParquetMessageTypeBuilder;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.settings.FileValue;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.adapter.parquet.shared.schema.inference.FieldSchema;
import org.polypheny.db.adapter.parquet.shared.schema.inference.SchemaState;
import org.polypheny.db.adapter.parquet.shared.schema.inference.ValueSchema;

/**
 * Parquet-writing helper layer
 */
public final class ParquetWorkflowLoadSupport {

    public static final String MODE_FAIL = "fail";
    public static final String MODE_DROP = "drop";
    public static final String COMPRESSION_SNAPPY = ParquetSourceWriter.COMPRESSION_SNAPPY;
    public static final String COMPRESSION_GZIP = ParquetSourceWriter.COMPRESSION_GZIP;
    public static final String COMPRESSION_UNCOMPRESSED = ParquetSourceWriter.COMPRESSION_UNCOMPRESSED;
    public static final String CONFLICT_STRINGIFY = SchemaState.CONFLICT_STRINGIFY;
    public static final String CONFLICT_FAIL = SchemaState.CONFLICT_FAIL;

    private static final int DEFAULT_PROGRESS_DELTA = 100;
    private static final String SCHEMA_NAME = "polypheny_workflow";

    private static final int MAX_NAME_LENGTH = 40;
    private static final int TRUNCATED_LENGTH = 37;


    private ParquetWorkflowLoadSupport() {
    }


    /**
     * Create dynamic name for load activity node.
     *
     * @param file target parquet file
     * @return activity display name
     */
    public static String getDynamicName( FileValue file ) throws IOException {
        //String name = file.getName();
        String name = file.getFile( false, false ).getName();
        if ( name.length() > MAX_NAME_LENGTH ) {
            name = name.substring( 0, TRUNCATED_LENGTH ) + "...";
        }
        return "Load to " + name;
    }


    /**
     * Check output path before Parquet writing starts
     *
     * @param file - parquet output file to write
     * @param mode - write mode
     */
    public static void prepareTargetFile( File file, String mode ) {
        // do nothing if file does not exist - writing can proceed
        if ( file.exists() ) {
            switch ( mode ) {
                // file exists -> exception
                case MODE_FAIL -> throw new GenericRuntimeException( "Specified file already exists." );
                // file exists -> delete it
                case MODE_DROP -> {
                    if ( !file.delete() ) {
                        throw new GenericRuntimeException( "Failed to delete existing file." );
                    }
                }
                default -> throw new IllegalArgumentException( "Unknown mode: " + mode );
            }
        }
    }


    /**
     * Write a relational workflow input into a Parquet file
     *
     * @param input - input pipe
     * @param file - file to write
     * @param compression - string
     * @param schemaSampleSize - int
     * @param conflictMode - String
     * @param keepPk - boolean
     * @param estimatedTupleCount - long
     * @param ctx - PipeExecutionContext
     * @throws Exception - exception
     */
    public static void writeRelational(
            InputPipe input,
            File file,
            String compression,
            int schemaSampleSize,
            String conflictMode,
            boolean keepPk,
            long estimatedTupleCount,
            PipeExecutionContext ctx ) throws Exception {

        // get relational input type
        AlgDataType inputType = input.getType();
        Iterator<List<PolyValue>> iterator = input.iterator();
        List<List<PolyValue>> sampleRows = new ArrayList<>(); // list of rows

        // create and initialize schema state
        SchemaState schemaState = new SchemaState( conflictMode );
        schemaState.init( inputType, keepPk );

        // read sample rows and check schema against real values
        while ( iterator.hasNext() && sampleRows.size() < schemaSampleSize ) {
            List<PolyValue> row = iterator.next();
            sampleRows.add( row );
            schemaState.mergeRelationalRowSchema( row, inputType, keepPk );
        }

        // write all rows to the file
        MessageType schema = new ParquetMessageTypeBuilder( schemaState, SCHEMA_NAME ).build();
        // write data to parquet file
        long countDelta = estimatedTupleCount > 0 ? Math.max( estimatedTupleCount / 100, 1 ) : DEFAULT_PROGRESS_DELTA;
        try ( ParquetSourceWriter writer = new ParquetSourceWriter( file, schema, compression, schemaState, false ) ) {
            writer.writeRows(
                    new BufferedIterator<>( sampleRows, iterator, r -> r ),
                    written -> updateProgress( estimatedTupleCount, countDelta, ctx, written ) );
        }

        if ( estimatedTupleCount > 0 ) {
            ctx.updateProgress( 1.0 );
        }
    }


    /**
     * Write document input to parquet file
     *
     * @param input - input pipe
     * @param file - to write
     * @param compression - data compression
     * @param schemaSampleSize - int
     * @param conflictMode - string
     * @param keepId - boolean
     * @param estimatedTupleCount - long
     * @param ctx - context
     * @throws Exception - exception
     */
    public static void writeDocuments(
            InputPipe input,
            File file,
            String compression,
            int schemaSampleSize,
            String conflictMode,
            boolean keepId,
            long estimatedTupleCount,
            PipeExecutionContext ctx ) throws Exception {

        Iterator<List<PolyValue>> iterator = input.iterator();
        List<PolyDocument> sampleDocs = new ArrayList<>();
        SchemaState schemaState = new SchemaState( conflictMode );

        // read sample rows and check schema against real values
        while ( iterator.hasNext() && sampleDocs.size() < schemaSampleSize ) {
            List<PolyValue> row = iterator.next();
            PolyDocument document = row.get( 0 ).asDocument();
            sampleDocs.add( document );
            schemaState.mergeDocumentSchema( document, keepId );
        }

        if ( schemaState.getFields().isEmpty() && keepId ) {
            schemaState.addField( new FieldSchema( Activity.docId.value, Activity.docId.value, -1, false, ValueSchema.stringType() ) );
        }

        MessageType schema = new ParquetMessageTypeBuilder( schemaState, SCHEMA_NAME ).build();
        long countDelta = estimatedTupleCount > 0 ? Math.max( estimatedTupleCount / 100, 1 ) : DEFAULT_PROGRESS_DELTA;
        try ( ParquetSourceWriter writer = new ParquetSourceWriter( file, schema, compression, schemaState, keepId ) ) {
            writer.writeRows(
                    new BufferedIterator<>( sampleDocs, iterator, r -> r.get( 0 ).asDocument() ),
                    written -> updateProgress( estimatedTupleCount, countDelta, ctx, written ) );
        }

        if ( estimatedTupleCount > 0 ) {
            ctx.updateProgress( 1.0 );
        }
    }


    private static void updateProgress( long estimatedTupleCount, long countDelta, PipeExecutionContext ctx, long written ) {
        if ( written % countDelta == 0 ) {
            if ( estimatedTupleCount > 0 ) {
                ctx.updateProgress( (double) written / estimatedTupleCount );
            }
            try {
                ctx.checkPipeInterrupted();
            } catch ( Exception e ) {
                throw new GenericRuntimeException( "Parquet write interrupted", e );
            }
        }
    }

}
