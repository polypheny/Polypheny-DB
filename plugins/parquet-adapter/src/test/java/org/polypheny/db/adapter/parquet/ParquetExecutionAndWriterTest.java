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

package org.polypheny.db.adapter.parquet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.polypheny.db.adapter.parquet.document.execution.ParquetDocValueExtractor;
import org.polypheny.db.adapter.parquet.relational.execution.FilterableParquetSourceFile;
import org.polypheny.db.adapter.parquet.relational.execution.ParquetRelEnumerator;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnBinding;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnRole;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetSourceFile;
import org.polypheny.db.adapter.parquet.shared.execution.BufferedIterator;
import org.polypheny.db.adapter.parquet.shared.execution.CombinedGroup;
import org.polypheny.db.adapter.parquet.shared.execution.VirtualGroup;
import org.polypheny.db.adapter.parquet.shared.filter.FiltersContainer;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetPrimitiveValueFilterEvaluator;
import org.polypheny.db.adapter.parquet.shared.io.OutputLocalFile;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSourceReader;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSourceWriter;
import org.polypheny.db.adapter.parquet.shared.schema.ParquetMessageTypeBuilder;
import org.polypheny.db.adapter.parquet.shared.schema.inference.SchemaState;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;
import org.polypheny.db.util.Sources;

class ParquetExecutionAndWriterTest {

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
    void bufferedIteratorReplaysSampleRowsBeforeConvertedRemainder() {
        BufferedIterator<Integer, String> iterator = new BufferedIterator<>( List.of( "sample-1", "sample-2" ), List.of( 3, 4 ).iterator(), value -> "row-" + value );

        assertTrue( iterator.hasNext() );
        assertEquals( "sample-1", iterator.next() );
        assertEquals( "sample-2", iterator.next() );
        assertEquals( "row-3", iterator.next() );
        assertEquals( "row-4", iterator.next() );
        assertFalse( iterator.hasNext() );
    }


    @Test
    void virtualGroupDelegatesToSourceGroupAndKeepsMetadata() {
        MessageType schema = Types.buildMessage()
                .required( PrimitiveTypeName.INT64 ).named( "id" )
                .optional( PrimitiveTypeName.BINARY ).as( org.apache.parquet.schema.LogicalTypeAnnotation.stringType() ).named( "name" )
                .named( "row" );
        Group source = new SimpleGroupFactory( schema ).newGroup()
                .append( "id", 7L )
                .append( "name", "Alice" );
        VirtualGroup group = new VirtualGroup( source, "row-7", "parent-1", 3 );

        assertEquals( 7L, group.getLong( 0, 0 ) );
        assertEquals( "Alice", group.getString( 1, 0 ) );
        assertEquals( "row-7", group.getMetadata().getRowId() );
        assertEquals( "parent-1", group.getMetadata().getParentRowId() );
        assertEquals( 3L, group.getMetadata().getOrdinal() );
        assertSame( schema, group.getType() );
    }


    @Test
    void combinedGroupRoutesFieldsByJoinSideAndRejectsIndexedAccess() {
        VirtualGroup parent = virtualGroup( "parent" );
        VirtualGroup child = virtualGroup( "child" );
        ParquetColumnBinding parentColumn = new ParquetColumnBinding( 1L, "parent_id", ParquetColumnRole.DATA, List.of( "parent_id" ) );
        ParquetColumnBinding childColumn = new ParquetColumnBinding( 2L, "child_id", ParquetColumnRole.DATA, List.of( "child_id" ) );
        CombinedGroup combined = new CombinedGroup( parent, List.of( parentColumn ), List.of( "orders" ), child, List.of( childColumn ), List.of( "orders", "items" ) );

        assertEquals( 2, combined.fieldCount() );
        assertSame( parent, combined.groupForField( 0, true ) );
        assertSame( child, combined.groupForField( 1, true ) );
        assertSame( child, combined.groupForField( 0, false ) );
        assertSame( parent, combined.groupForField( 1, false ) );
        assertEquals( parentColumn, combined.bindingForField( 0, true ) );
        assertEquals( childColumn, combined.bindingForField( 0, false ) );
        assertEquals( List.of( "orders" ), combined.tablePathForField( 0, true ) );
        assertEquals( List.of( "orders", "items" ), combined.tablePathForField( 0, false ) );
        assertFalse( combined.isNullField( 0, true ) );
        assertSame( parent.getType(), combined.getType() );
        assertThrows( UnsupportedOperationException.class, () -> combined.getInteger( 0, 0 ) );

        CombinedGroup unmatchedChild = new CombinedGroup( parent, List.of( parentColumn ), List.of(), null, List.of( childColumn ), List.of() );
        assertTrue( unmatchedChild.isNullField( 1, true ) );
    }


    @Test
    void filterableSourceFileDefensivelyCopiesFilters() {
        ParquetSourceFile sourceFile = new ParquetSourceFile( "file:/tmp/source.parquet", Map.of(), Map.of() );
        ParquetAdapterFilter<PolyValue> filter = new ParquetAdapterFilter<>( 0, Kind.EQUALS, PolyString.of( "EU" ) );
        List<ParquetAdapterFilter<PolyValue>> filters = new ArrayList<>( List.of( filter ) );

        FilterableParquetSourceFile filterable = new FilterableParquetSourceFile( sourceFile, filters );
        filters.clear();

        assertEquals( List.of( filter ), filterable.filters() );
        assertThrows( UnsupportedOperationException.class, () -> filterable.filters().add( filter ) );
    }


    @Test
    void primitiveValueFilterEvaluatorHandlesScalarComparisonsAndUnknownIndexes() {
        ParquetPrimitiveValueFilterEvaluator evaluator = new ParquetPrimitiveValueFilterEvaluator();
        Object[] values = new Object[]{ 10, true, Binary.fromString( "alpha" ), null };

        assertEquals( Boolean.TRUE, evaluator.evaluate( values, new ParquetAdapterFilter<>( 0, Kind.GREATER_THAN, PolyInteger.of( 7 ) ) ) );
        assertEquals( Boolean.FALSE, evaluator.evaluate( values, new ParquetAdapterFilter<>( 0, Kind.LESS_THAN, PolyInteger.of( 7 ) ) ) );
        assertEquals( Boolean.TRUE, evaluator.evaluate( values, new ParquetAdapterFilter<>( 1, Kind.EQUALS, PolyBoolean.of( true ) ) ) );
        assertEquals( Boolean.TRUE, evaluator.evaluate( values, new ParquetAdapterFilter<>( 2, Kind.EQUALS, PolyString.of( "alpha" ) ) ) );
        assertEquals( Boolean.TRUE, evaluator.evaluate( values, new ParquetAdapterFilter<>( 3, Kind.IS_NULL, null ) ) );
        assertEquals( Boolean.FALSE, evaluator.evaluate( values, new ParquetAdapterFilter<>( 3, Kind.IS_NOT_NULL, null ) ) );
        assertEquals( Boolean.TRUE, evaluator.evaluate( values, new ParquetAdapterFilter<>( 0, Kind.PLUS, PolyInteger.of( 99 ) ) ) );
        assertNull( evaluator.evaluate( values, new ParquetAdapterFilter<>( 99, Kind.EQUALS, PolyInteger.of( 1 ) ) ) );
    }


    @Test
    void sourceWriterRoundTripsRelationalRowsThroughRelEnumerator() throws Exception {
        File file = tempDir.resolve( "writer-rel.parquet" ).toFile();
        AlgDataType inputType = AlgDataTypeFactory.DEFAULT.builder()
                .add( "id", null, PolyType.BIGINT )
                .add( "Name", null, PolyType.VARCHAR )
                .add( "Score", null, PolyType.INTEGER )
                .build();
        SchemaState schemaState = new SchemaState( SchemaState.CONFLICT_FAIL );
        schemaState.init( inputType, true );
        schemaState.mergeRelationalRowSchema( List.of( PolyLong.of( 101 ), PolyString.of( "Alice" ), PolyInteger.of( 9 ) ), inputType, true );
        MessageType schema = new ParquetMessageTypeBuilder( schemaState, "writer_rel" ).build();
        List<Long> progress = new ArrayList<>();

        try ( ParquetSourceWriter writer = new ParquetSourceWriter( new OutputLocalFile( file ), schema, ParquetSourceWriter.COMPRESSION_UNCOMPRESSED, schemaState, false ) ) {
            writer.writeRows( List.of(
                    List.of( PolyLong.of( 101 ), PolyString.of( "Alice" ), PolyInteger.of( 9 ) ),
                    List.of( PolyLong.of( 102 ), PolyString.of( "Bob" ), PolyInteger.of( 7 ) ) ).iterator(), progress::add );
        }

        assertEquals( List.of( 1L, 2L ), progress );
        try ( ParquetSourceReader reader = new ParquetSourceReader( Sources.of( file ) ) ) {
            ParquetRelEnumerator enumerator = new ParquetRelEnumerator( reader, FiltersContainer.empty, false );
            assertTrue( enumerator.moveNext() );
            PolyValue[] first = enumerator.current();
            assertEquals( 101L, first[0].asNumber().longValue() );
            assertEquals( "Alice", first[1].asString().value );
            assertEquals( 9, first[2].asNumber().intValue() );
            assertTrue( enumerator.moveNext() );
            assertEquals( "Bob", enumerator.current()[1].asString().value );
            assertFalse( enumerator.moveNext() );
            enumerator.close();
        }
    }


    @Test
    void sourceWriterRoundTripsDocumentsAndGeneratesMissingIds() throws Exception {
        File file = tempDir.resolve( "writer-doc.parquet" ).toFile();
        PolyDocument document = PolyDocument.ofDocument( Map.of(
                PolyString.of( DocumentType.DOCUMENT_ID ), PolyString.of( "doc-1" ),
                PolyString.of( "Name" ), PolyString.of( "Alice" ),
                PolyString.of( "Tags" ), PolyList.of( List.of( PolyString.of( "red" ), PolyString.of( "blue" ) ) ) ) );
        SchemaState schemaState = new SchemaState( SchemaState.CONFLICT_FAIL );
        schemaState.mergeDocumentSchema( document, false );
        MessageType schema = new ParquetMessageTypeBuilder( schemaState, "writer_doc" ).build();

        try ( ParquetSourceWriter writer = new ParquetSourceWriter( new OutputLocalFile( file ), schema, ParquetSourceWriter.COMPRESSION_UNCOMPRESSED, schemaState, false ) ) {
            writer.writeRows( List.of( document ).iterator(), null );
        }

        try ( ParquetSourceReader reader = new ParquetSourceReader( Sources.of( file ) ) ) {
            Group group = reader.next();
            PolyDocument read = new ParquetDocValueExtractor().extractDocument( group, reader.getProjectionSchema(), PolyString.of( "generated-id" ) );

            assertEquals( "Alice", read.get( PolyString.of( "name" ) ).asString().value );
            assertEquals( "red", read.get( PolyString.of( "tags" ) ).asList().get( 0 ).asString().value );
            assertEquals( "blue", read.get( PolyString.of( "tags" ) ).asList().get( 1 ).asString().value );
            assertEquals( "generated-id", read.get( PolyString.of( DocumentType.DOCUMENT_ID ) ).asString().value );
        }
    }


    @Test
    void sourceWriterReportsCompressionAndValueConversionFailures() throws Exception {
        File badCompression = tempDir.resolve( "bad-compression.parquet" ).toFile();
        AlgDataType inputType = AlgDataTypeFactory.DEFAULT.builder()
                .add( "amount", null, PolyType.INTEGER )
                .build();
        SchemaState schemaState = new SchemaState( SchemaState.CONFLICT_FAIL );
        schemaState.init( inputType, true );
        MessageType schema = new ParquetMessageTypeBuilder( schemaState, "writer_errors" ).build();

        //noinspection resource
        assertThrows( GenericRuntimeException.class, () -> new ParquetSourceWriter( new OutputLocalFile( badCompression ), schema, "brotli", schemaState, false ) );

        try ( ParquetSourceWriter writer = new ParquetSourceWriter( new OutputLocalFile( tempDir.resolve( "bad-value.parquet" ).toFile() ), schema, ParquetSourceWriter.COMPRESSION_UNCOMPRESSED, schemaState, false ) ) {
            assertThrows( GenericRuntimeException.class, () -> writer.writeRows( List.of( List.<PolyValue>of( PolyString.of( "not-a-number" ) ) ).iterator(), null ) );
        }
    }


    private static VirtualGroup virtualGroup( String id ) {
        MessageType schema = Types.buildMessage()
                .required( PrimitiveTypeName.INT64 ).named( "id" )
                .named( "row" );
        Group source = new SimpleGroupFactory( schema ).newGroup().append( "id", 1L );
        return new VirtualGroup( source, id, null, 0 );
    }

}
