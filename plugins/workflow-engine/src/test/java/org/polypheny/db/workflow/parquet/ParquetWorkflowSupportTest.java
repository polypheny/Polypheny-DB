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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.algebra.type.GraphType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyBinary;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.type.entity.temporal.PolyDate;
import org.polypheny.db.type.entity.temporal.PolyTime;
import org.polypheny.db.type.entity.temporal.PolyTimestamp;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;
import org.polypheny.db.util.Sources;
import org.polypheny.db.util.temporal.DateTimeUtils;
import org.polypheny.db.workflow.dag.activities.Activity;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidInputException;
import org.polypheny.db.workflow.dag.activities.TypePreview.RelType;
import org.polypheny.db.workflow.dag.activities.impl.extract.ParquetExtractActivity;
import org.polypheny.db.workflow.dag.activities.impl.load.ParquetLoadActivity;
import org.polypheny.db.workflow.dag.settings.BoolValue;
import org.polypheny.db.workflow.dag.settings.FileValue;
import org.polypheny.db.workflow.dag.settings.FileValue.SourceType;
import org.polypheny.db.workflow.dag.settings.IntValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingsPreview;
import org.polypheny.db.workflow.dag.settings.StringValue;
import org.polypheny.db.workflow.dag.variables.ReadableVariableStore;
import org.polypheny.db.workflow.engine.execution.context.PipeExecutionContext;
import org.polypheny.db.workflow.engine.execution.pipe.InputPipe;
import org.polypheny.db.workflow.engine.execution.pipe.OutputPipe;

class ParquetWorkflowSupportTest {

    @TempDir
    Path tempDir;


    @BeforeAll
    static void initHomeDir() {
        try {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        } catch ( Exception e ) {
            // Already initialized by another test.
        }
    }


    @Test
    void prepareTargetFileFailsOrDeletesExistingFile() throws Exception {
        Path target = tempDir.resolve( "target.parquet" );
        Files.writeString( target, "old" );

        assertThrows( GenericRuntimeException.class, () -> ParquetWorkflowLoadSupport.prepareTargetFile( target.toFile(), ParquetWorkflowLoadSupport.MODE_FAIL ) );
        assertTrue( Files.exists( target ) );

        ParquetWorkflowLoadSupport.prepareTargetFile( target.toFile(), ParquetWorkflowLoadSupport.MODE_DROP );
        assertFalse( Files.exists( target ) );

        Files.writeString( target, "old" );
        assertThrows( IllegalArgumentException.class, () -> ParquetWorkflowLoadSupport.prepareTargetFile( target.toFile(), "unknown" ) );
    }


    @Test
    void relationalWriteAndExtractRoundTripHonorsPrimaryKeyFileNameAndMaxCount() throws Exception {
        File target = tempDir.resolve( "relational.parquet" ).toFile();
        AlgDataType inputType = AlgDataTypeFactory.DEFAULT.builder()
                .add( Activity.PK_COL, null, PolyType.BIGINT )
                .add( "Customer Name", null, PolyType.VARCHAR )
                .add( "Score", null, PolyType.INTEGER )
                .build();
        TestPipeExecutionContext ctx = new TestPipeExecutionContext( List.of( 2L ) );

        ParquetWorkflowLoadSupport.writeRelational(
                new TestInputPipe( inputType, List.of(
                        List.of( PolyLong.of( 101 ), PolyString.of( "Alice" ), PolyInteger.of( 7 ) ),
                        List.of( PolyLong.of( 102 ), PolyString.of( "Bob" ), PolyInteger.of( 9 ) ) ) ),
                target,
                ParquetWorkflowLoadSupport.COMPRESSION_UNCOMPRESSED,
                2,
                ParquetWorkflowLoadSupport.CONFLICT_FAIL,
                false,
                2,
                ctx );

        AlgDataType outputType = ParquetWorkflowExtractSupport.getOutputType( Sources.of( target ), ParquetWorkflowExtractSupport.OUTPUT_RELATIONAL, true );
        CollectingOutputPipe output = new CollectingOutputPipe( outputType );
        ParquetWorkflowExtractSupport.writeRows( output, Sources.of( target ), true, 1 );

        assertEquals( List.of( Activity.PK_COL, "customer_name", "score", "fileName" ), outputType.getFieldNames() );
        assertEquals( List.of( 0.5D, 1.0D, 1.0D ), ctx.progress );
        assertEquals( 1, output.rows.size() );
        List<PolyValue> row = output.rows.get( 0 );
        assertEquals( 0L, row.get( 0 ).asNumber().longValue() );
        assertEquals( "Alice", row.get( 1 ).asString().value );
        assertEquals( 7, row.get( 2 ).asNumber().intValue() );
        assertEquals( target.getName(), row.get( 3 ).asString().value );
    }


    @Test
    void documentWriteAndExtractRoundTripHonorsKeepIdNameFieldAndMaxCount() throws Exception {
        File target = tempDir.resolve( "documents.parquet" ).toFile();
        PolyDocument first = doc( Map.of(
                Activity.docId, PolyString.of( "doc-1" ),
                PolyString.of( "Name" ), PolyString.of( "Alice" ),
                PolyString.of( "Details" ), doc( Map.of( PolyString.of( "city" ), PolyString.of( "Berlin" ) ) ) ) );
        PolyDocument second = doc( Map.of(
                Activity.docId, PolyString.of( "doc-2" ),
                PolyString.of( "Name" ), PolyString.of( "Bob" ) ) );

        ParquetWorkflowLoadSupport.writeDocuments(
                new TestInputPipe( DocumentType.ofId(), List.of( List.of( first ), List.of( second ) ) ),
                target,
                ParquetWorkflowLoadSupport.COMPRESSION_UNCOMPRESSED,
                2,
                ParquetWorkflowLoadSupport.CONFLICT_FAIL,
                false,
                2,
                new TestPipeExecutionContext( List.of( 2L ) ) );

        CollectingOutputPipe output = new CollectingOutputPipe( DocumentType.ofId() );
        ParquetWorkflowExtractSupport.writeDocuments( output, Sources.of( target ), true, 1 );

        assertEquals( 1, output.rows.size() );
        PolyDocument document = output.rows.get( 0 ).get( 0 ).asDocument();
        assertEquals( "Alice", document.get( PolyString.of( "name" ) ).asString().value );
        assertEquals( "Berlin", document.get( PolyString.of( "details" ) ).asDocument().get( PolyString.of( "city" ) ).asString().value );
        assertNotEquals( "doc-1", document.get( Activity.docId ).asString().value );
        assertTrue( document.get( Activity.docId ).asString().value.endsWith( target.getName() + "#0" ) );
        assertEquals( target.getName(), document.get( PolyString.of( "fileName" ) ).asString().value );
    }


    @Test
    void loadActivityPipeWritesRelationalWithKeptPrimaryKeyAndGzip() throws Exception {
        File target = tempDir.resolve( "activity-relational.parquet" ).toFile();
        AlgDataType inputType = AlgDataTypeFactory.DEFAULT.builder()
                .add( Activity.PK_COL, null, PolyType.BIGINT )
                .add( "Name", null, PolyType.VARCHAR )
                .build();
        ParquetLoadActivity activity = new ParquetLoadActivity();

        activity.pipe(
                List.of( new TestInputPipe( inputType, List.of( List.of( PolyLong.of( 7 ), PolyString.of( "Alice" ) ) ) ) ),
                null,
                loadSettings( target, ParquetWorkflowLoadSupport.COMPRESSION_GZIP, ParquetWorkflowLoadSupport.CONFLICT_FAIL, true, true ),
                new TestPipeExecutionContext( List.of( 1L ) ) );

        assertTrue( target.isFile() );
        AlgDataType outputType = ParquetWorkflowExtractSupport.getOutputType( Sources.of( target ), ParquetWorkflowExtractSupport.OUTPUT_RELATIONAL, false );
        CollectingOutputPipe output = new CollectingOutputPipe( outputType );
        ParquetWorkflowExtractSupport.writeRows( output, Sources.of( target ), false, -1 );

        assertEquals( 3, outputType.getFieldCount() );
        assertTrue( outputType.getFieldNames().contains( "name" ) );
        assertEquals( 1, output.rows.size() );
        assertEquals( 0L, output.rows.get( 0 ).get( 0 ).asNumber().longValue() );
        assertEquals( 7L, output.rows.get( 0 ).get( 1 ).asNumber().longValue() );
        assertEquals( "Alice", output.rows.get( 0 ).get( 2 ).asString().value );
    }


    @Test
    void relationalWriterRoundTripsTemporalAndBinaryValues() throws Exception {
        File target = tempDir.resolve( "temporal-binary.parquet" ).toFile();
        AlgDataType inputType = AlgDataTypeFactory.DEFAULT.builder()
                .add( Activity.PK_COL, null, PolyType.BIGINT )
                .add( "Created Date", null, PolyType.DATE )
                .add( "Created Time", null, PolyType.TIME )
                .add( "Updated At", null, PolyType.TIMESTAMP )
                .add( "Payload", null, PolyType.VARBINARY )
                .build();

        ParquetWorkflowLoadSupport.writeRelational(
                new TestInputPipe( inputType, List.of( List.of(
                        PolyLong.of( 1 ),
                        PolyDate.of( 19000L * DateTimeUtils.MILLIS_PER_DAY ),
                        PolyTime.of( 12345 ),
                        PolyTimestamp.of( 1609459200123L ),
                        PolyBinary.of( new byte[]{ 1, 2, 3 } ) ) ) ),
                target,
                ParquetWorkflowLoadSupport.COMPRESSION_UNCOMPRESSED,
                1,
                ParquetWorkflowLoadSupport.CONFLICT_FAIL,
                false,
                1,
                new TestPipeExecutionContext( List.of( 1L ) ) );

        AlgDataType outputType = ParquetWorkflowExtractSupport.getOutputType( Sources.of( target ), ParquetWorkflowExtractSupport.OUTPUT_RELATIONAL, false );
        CollectingOutputPipe output = new CollectingOutputPipe( outputType );
        ParquetWorkflowExtractSupport.writeRows( output, Sources.of( target ), false, -1 );
        List<PolyValue> row = output.rows.get( 0 );

        assertEquals( List.of( Activity.PK_COL, "created_date", "created_time", "updated_at", "payload" ), outputType.getFieldNames() );
        assertEquals( 19000L, row.get( 1 ).asDate().getDaysSinceEpoch() );
        assertEquals( 12345, row.get( 2 ).asTime().ofDay );
        assertEquals( 1609459200123L, row.get( 3 ).asTimestamp().millisSinceEpoch );
        assertArrayEquals( new byte[]{ 1, 2, 3 }, row.get( 4 ).asBinary().value );
    }


    @Test
    void loadActivityPipeRejectsGraphInputAtRuntime() {
        ParquetLoadActivity activity = new ParquetLoadActivity();
        File target = tempDir.resolve( "graph.parquet" ).toFile();

        assertThrows( InvalidInputException.class, () -> activity.pipe(
                List.of( new TestInputPipe( GraphType.of(), List.of() ) ),
                null,
                loadSettings( target, ParquetWorkflowLoadSupport.COMPRESSION_UNCOMPRESSED, ParquetWorkflowLoadSupport.CONFLICT_FAIL, true, true ),
                new TestPipeExecutionContext( List.of() ) ) );
    }


    @Test
    void loadActivityPipeFailsWhenSampleConflictsWithDeclaredRelationalSchema() {
        ParquetLoadActivity activity = new ParquetLoadActivity();
        File target = tempDir.resolve( "conflict.parquet" ).toFile();
        AlgDataType inputType = AlgDataTypeFactory.DEFAULT.builder()
                .add( Activity.PK_COL, null, PolyType.BIGINT )
                .add( "Amount", null, PolyType.INTEGER )
                .build();

        assertThrows( GenericRuntimeException.class, () -> activity.pipe(
                List.of( new TestInputPipe( inputType, List.of( List.of( PolyLong.of( 1 ), PolyString.of( "not-an-int" ) ) ) ) ),
                null,
                loadSettings( target, ParquetWorkflowLoadSupport.COMPRESSION_UNCOMPRESSED, ParquetWorkflowLoadSupport.CONFLICT_FAIL, true, false ),
                new TestPipeExecutionContext( List.of( 1L ) ) ) );
    }


    @Test
    void extractActivityLocksOutputTypeAndPipesMultipleSources() throws Exception {
        Path sourceDir = tempDir.resolve( "sources" );
        Files.createDirectories( sourceDir );
        AlgDataType inputType = AlgDataTypeFactory.DEFAULT.builder()
                .add( Activity.PK_COL, null, PolyType.BIGINT )
                .add( "Name", null, PolyType.VARCHAR )
                .build();
        ParquetWorkflowLoadSupport.writeRelational(
                new TestInputPipe( inputType, List.of( List.of( PolyLong.of( 1 ), PolyString.of( "Alice" ) ) ) ),
                sourceDir.resolve( "first.parquet" ).toFile(),
                ParquetWorkflowLoadSupport.COMPRESSION_UNCOMPRESSED,
                1,
                ParquetWorkflowLoadSupport.CONFLICT_FAIL,
                false,
                1,
                new TestPipeExecutionContext( List.of( 1L ) ) );
        ParquetWorkflowLoadSupport.writeRelational(
                new TestInputPipe( inputType, List.of( List.of( PolyLong.of( 2 ), PolyString.of( "Bob" ) ) ) ),
                sourceDir.resolve( "second.parquet" ).toFile(),
                ParquetWorkflowLoadSupport.COMPRESSION_UNCOMPRESSED,
                1,
                ParquetWorkflowLoadSupport.CONFLICT_FAIL,
                false,
                1,
                new TestPipeExecutionContext( List.of( 1L ) ) );
        ParquetExtractActivity activity = new ParquetExtractActivity();
        Settings settings = extractSettings( sourceDir, ParquetWorkflowExtractSupport.OUTPUT_RELATIONAL, true, -1 );

        AlgDataType outputType = activity.lockOutputType( List.of(), settings );
        CollectingOutputPipe output = new CollectingOutputPipe( outputType );
        activity.pipe( List.of(), output, settings, new TestPipeExecutionContext( List.of() ) );

        assertEquals( List.of( Activity.PK_COL, "name", "fileName" ), outputType.getFieldNames() );
        assertEquals( 2L, activity.estimateTupleCount( List.of(), settings, List.of(), () -> null ) );
        assertEquals( 2, output.rows.size() );
        Map<String, String> namesByFile = new LinkedHashMap<>();
        for ( List<PolyValue> row : output.rows ) {
            namesByFile.put( row.get( 2 ).asString().value, row.get( 1 ).asString().value );
        }
        assertEquals( Map.of( "first.parquet", "Alice", "second.parquet", "Bob" ), namesByFile );
    }


    @Test
    void documentWriteKeepsOnlyIdWhenRequestedAndRejectsEmptyDroppedIdSchema() throws Exception {
        File target = tempDir.resolve( "only-id.parquet" ).toFile();
        PolyDocument onlyId = doc( Map.of( Activity.docId, PolyString.of( "doc-1" ) ) );

        ParquetWorkflowLoadSupport.writeDocuments(
                new TestInputPipe( DocumentType.ofId(), List.of( List.of( onlyId ) ) ),
                target,
                ParquetWorkflowLoadSupport.COMPRESSION_UNCOMPRESSED,
                1,
                ParquetWorkflowLoadSupport.CONFLICT_FAIL,
                true,
                1,
                new TestPipeExecutionContext( List.of( 1L ) ) );

        CollectingOutputPipe output = new CollectingOutputPipe( DocumentType.ofId() );
        ParquetWorkflowExtractSupport.writeDocuments( output, Sources.of( target ), false, -1 );
        assertEquals( "doc-1", output.rows.get( 0 ).get( 0 ).asDocument().get( Activity.docId ).asString().value );

        assertThrows( GenericRuntimeException.class, () -> ParquetWorkflowLoadSupport.writeDocuments(
                new TestInputPipe( DocumentType.ofId(), List.of( List.of( onlyId ) ) ),
                tempDir.resolve( "empty-doc.parquet" ).toFile(),
                ParquetWorkflowLoadSupport.COMPRESSION_UNCOMPRESSED,
                1,
                ParquetWorkflowLoadSupport.CONFLICT_FAIL,
                false,
                1,
                new TestPipeExecutionContext( List.of( 1L ) ) ) );
    }


    @Test
    void loadActivityPreviewRejectsEmptyRelationalExportAndBuildsDynamicName() {
        ParquetLoadActivity activity = new ParquetLoadActivity();
        AlgDataType onlyPk = AlgDataTypeFactory.DEFAULT.builder()
                .add( Activity.PK_COL, null, PolyType.BIGINT )
                .build();
        SettingsPreview keepPkFalse = new SettingsPreview( Map.of( "keepPk", Optional.of( new BoolValue( false ) ) ) );
        SettingsPreview keepPkTrue = new SettingsPreview( Map.of( "keepPk", Optional.of( new BoolValue( true ) ) ) );
        Path longTarget = tempDir.resolve( "abcdefghijklmnopqrstuvwxyz0123456789-extra.parquet" );
        SettingsPreview fileSettings = new SettingsPreview( Map.of( "file", Optional.of( new FileValue( longTarget.toString(), SourceType.ABS_FILE, false ) ) ) );

        assertThrows( InvalidInputException.class, () -> activity.previewOutTypes( List.of( RelType.of( onlyPk ) ), keepPkFalse ) );
        assertDoesNotThrow( () -> activity.previewOutTypes( List.of( RelType.of( onlyPk ) ), keepPkTrue ) );
        assertTrue( activity.getDynamicName( List.of(), fileSettings ).startsWith( "Load to abcdefghijklmnopqrstuvwxyz" ) );
    }


    @Test
    void extractSupportBuildsDynamicNamesAndDocumentOutputType() throws Exception {
        File target = tempDir.resolve( "single.parquet" ).toFile();
        AlgDataType inputType = AlgDataTypeFactory.DEFAULT.builder()
                .add( Activity.PK_COL, null, PolyType.BIGINT )
                .add( "Name", null, PolyType.VARCHAR )
                .build();
        ParquetWorkflowLoadSupport.writeRelational(
                new TestInputPipe( inputType, List.of( List.of( PolyLong.of( 1 ), PolyString.of( "Alice" ) ) ) ),
                target,
                ParquetWorkflowLoadSupport.COMPRESSION_UNCOMPRESSED,
                1,
                ParquetWorkflowLoadSupport.CONFLICT_FAIL,
                false,
                1,
                new TestPipeExecutionContext( List.of( 1L ) ) );

        assertEquals( DocumentType.ofId(), ParquetWorkflowExtractSupport.getOutputType( Sources.of( target ), ParquetWorkflowExtractSupport.OUTPUT_DOCUMENT, false ) );
        assertEquals( "Extract Parquet Table: single.parquet", ParquetWorkflowExtractSupport.getDynamicName( ParquetWorkflowExtractSupport.OUTPUT_RELATIONAL, List.of( Sources.of( target ) ) ) );
        assertEquals( "Extract Parquet Documents", ParquetWorkflowExtractSupport.getDynamicName( ParquetWorkflowExtractSupport.OUTPUT_DOCUMENT, List.of( Sources.of( target ), Sources.of( target ) ) ) );
        assertEquals( 1L, ParquetWorkflowExtractSupport.estimateTupleCount( List.of( Sources.of( target ) ) ) );
    }


    private static PolyDocument doc( Map<PolyString, PolyValue> values ) {
        return PolyDocument.ofDocument( values );
    }


    private static Settings loadSettings( File file, String compression, String conflictMode, boolean keepId, boolean keepPk ) {
        Map<String, SettingValue> values = new LinkedHashMap<>();
        values.put( "file", new FileValue( file.getAbsolutePath(), SourceType.ABS_FILE, false ) );
        values.put( "mode", StringValue.of( ParquetWorkflowLoadSupport.MODE_FAIL ) );
        values.put( "compression", StringValue.of( compression ) );
        values.put( "schemaSampleSize", new IntValue( 1 ) );
        values.put( "conflictMode", StringValue.of( conflictMode ) );
        values.put( "keepId", new BoolValue( keepId ) );
        values.put( "keepPk", new BoolValue( keepPk ) );
        return new Settings( values );
    }


    private static Settings extractSettings( Path path, String outputModel, boolean nameField, int maxCount ) {
        Map<String, SettingValue> values = new LinkedHashMap<>();
        values.put( "file", new FileValue( path.toString(), SourceType.ABS_FILE, true ) );
        values.put( "outputModel", StringValue.of( outputModel ) );
        values.put( "nameField", new BoolValue( nameField ) );
        values.put( "maxCount", new IntValue( maxCount ) );
        return new Settings( values );
    }


    private record TestInputPipe( AlgDataType type, List<List<PolyValue>> rows ) implements InputPipe {

        @Override
        public AlgDataType getType() {
            return type;
        }


        @Override
        public @NotNull Iterator<List<PolyValue>> iterator() {
            return rows.iterator();
        }


        @Override
        public void finishIteration() {
        }

    }


    private static class CollectingOutputPipe implements OutputPipe {

        private final AlgDataType type;
        private final List<List<PolyValue>> rows = new java.util.ArrayList<>();


        private CollectingOutputPipe( AlgDataType type ) {
            this.type = type;
        }


        @Override
        public boolean put( List<PolyValue> value ) {
            rows.add( value );
            return true;
        }


        @Override
        public AlgDataType getType() {
            return type;
        }


        @Override
        public double getEstimatedProgress() {
            return -1;
        }


        @Override
        public void close() {
        }

    }


    private static class TestPipeExecutionContext implements PipeExecutionContext {

        private final List<Long> estimatedInCounts;
        private final List<Double> progress = new java.util.ArrayList<>();


        private TestPipeExecutionContext( List<Long> estimatedInCounts ) {
            this.estimatedInCounts = estimatedInCounts;
        }


        @Override
        public void checkPipeInterrupted() {
        }


        @Override
        public void updateProgress( double value ) {
            progress.add( value );
        }


        @Override
        public Transaction getTransaction() {
            return null;
        }


        @Override
        public ReadableVariableStore getVariableStore() {
            return null;
        }


        @Override
        public void logInfo( String message ) {
        }


        @Override
        public void logWarning( String message ) {
        }


        @Override
        public void logError( String message ) {
        }


        @Override
        public List<Long> getEstimatedInCounts() {
            return estimatedInCounts;
        }

    }

}
