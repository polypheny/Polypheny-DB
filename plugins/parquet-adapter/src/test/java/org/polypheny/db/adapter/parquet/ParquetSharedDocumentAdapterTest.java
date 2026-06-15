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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.polypheny.db.adapter.RelationalDataSource.ExportedColumn;
import org.polypheny.db.adapter.parquet.document.execution.ParquetDocEnumerator;
import org.polypheny.db.adapter.parquet.document.execution.ParquetDocFilterTranslator;
import org.polypheny.db.adapter.parquet.document.execution.ParquetDocValueExtractor;
import org.polypheny.db.adapter.parquet.shared.filter.FiltersContainer;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.io.ParquetSourceReader;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.nodes.SpecialOperator;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNameRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;
import org.polypheny.db.util.Sources;

class ParquetSharedDocumentAdapterTest {

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
    void extractDocumentNormalizesNestedValuesAndGeneratesMissingId() {
        MessageType schema = documentSchema();
        SimpleGroupFactory factory = new SimpleGroupFactory( schema );
        Group row = factory.newGroup()
                .append( "Full Name", "Alice" )
                .append( "Tags", "vip" )
                .append( "Tags", "north" );
        row.addGroup( "Meta Data" ).append( "Source Name", "crm" );

        PolyDocument document = new ParquetDocValueExtractor().extractDocument( row, schema, PolyString.of( "generated-id" ) );

        assertEquals( "Alice", document.get( PolyString.of( "full_name" ) ).asString().value );
        assertEquals( "crm", document.get( PolyString.of( "meta_data" ) ).asDocument().get( PolyString.of( "source_name" ) ).asString().value );
        PolyList<PolyValue> tags = document.get( PolyString.of( "tags" ) ).asList();
        assertEquals( 2, tags.size() );
        assertEquals( "vip", tags.get( 0 ).asString().value );
        assertEquals( "generated-id", document.get( PolyString.of( "_id" ) ).asString().value );
    }


    @Test
    void extractDocumentPreservesSourceIdAndGeneratesIdForAllNullRows() {
        MessageType schema = Types.buildMessage()
                .optional( PrimitiveTypeName.BINARY ).as( LogicalTypeAnnotation.stringType() ).named( "_id" )
                .optional( PrimitiveTypeName.BINARY ).as( LogicalTypeAnnotation.stringType() ).named( "Full Name" )
                .named( "doc_schema" );
        SimpleGroupFactory factory = new SimpleGroupFactory( schema );
        ParquetDocValueExtractor extractor = new ParquetDocValueExtractor();

        PolyDocument withId = extractor.extractDocument(
                factory.newGroup()
                        .append( "_id", "source-id" )
                        .append( "Full Name", "Alice" ),
                schema,
                PolyString.of( "generated-id" ) );
        PolyDocument allNull = extractor.extractDocument( factory.newGroup(), schema, PolyString.of( "generated-empty-id" ) );

        assertEquals( "source-id", withId.get( PolyString.of( "_id" ) ).asString().value );
        assertEquals( "Alice", withId.get( PolyString.of( "full_name" ) ).asString().value );
        assertEquals( 1, allNull.size() );
        assertEquals( "generated-empty-id", allNull.get( PolyString.of( "_id" ) ).asString().value );
    }


    @Test
    void extractDocumentConvertsRepeatedNestedGroupsToDocumentLists() {
        MessageType schema = Types.buildMessage()
                .repeatedGroup()
                .optional( PrimitiveTypeName.BINARY ).as( LogicalTypeAnnotation.stringType() ).named( "City" )
                .optional( PrimitiveTypeName.INT64 ).named( "Zip" )
                .named( "Addresses" )
                .named( "doc_schema" );
        SimpleGroupFactory factory = new SimpleGroupFactory( schema );
        Group row = factory.newGroup();
        row.addGroup( "Addresses" )
                .append( "City", "Berlin" )
                .append( "Zip", 10115L );
        row.addGroup( "Addresses" )
                .append( "City", "Zurich" )
                .append( "Zip", 8001L );

        PolyDocument document = new ParquetDocValueExtractor().extractDocument( row, schema, PolyString.of( "generated-id" ) );

        PolyList<PolyValue> addresses = document.get( PolyString.of( "addresses" ) ).asList();
        assertEquals( 2, addresses.size() );
        assertEquals( "Berlin", addresses.get( 0 ).asDocument().get( PolyString.of( "city" ) ).asString().value );
        assertEquals( 10115L, addresses.get( 0 ).asDocument().get( PolyString.of( "zip" ) ).asNumber().longValue() );
        assertEquals( "Zurich", addresses.get( 1 ).asDocument().get( PolyString.of( "city" ) ).asString().value );
    }


    @Test
    void docFilterTranslatorTranslatesTopLevelNameAndRejectsNestedPaths() {
        AlgDataTypeFactory typeFactory = new PolyTypeFactoryImpl( AlgDataTypeSystem.DEFAULT );
        AlgDataType stringType = typeFactory.createPolyType( PolyType.VARCHAR );
        AlgDataType boolType = typeFactory.createPolyType( PolyType.BOOLEAN );
        ParquetDocFilterTranslator translator = new ParquetDocFilterTranslator();
        List<ExportedColumn> columns = List.of( new ExportedColumn(
                "status",
                PolyType.TEXT,
                null,
                null,
                null,
                null,
                null,
                true,
                "documents",
                "documents",
                "status",
                3,
                false ) );

        ParquetAdapterFilter<?> translated = translator.translate(
                columns,
                call( boolType, Kind.EQUALS, new RexNameRef( "Status", null, stringType ), new RexDynamicParam( stringType, 4 ) ) );
        ParquetAdapterFilter<?> mqlGt = translator.translate(
                columns,
                call( boolType, Kind.GREATER_THAN, new RexNameRef( "status", null, stringType ), new RexDynamicParam( stringType, 6 ) ) );
        RexNode nested = call( boolType, Kind.EQUALS, new RexNameRef( List.of( "address", "city" ), null, stringType ), new RexDynamicParam( stringType, 5 ) );
        RexBuilder rexBuilder = new RexBuilder( typeFactory );
        RexCall path = new RexCall(
                typeFactory.createArrayType( typeFactory.createPolyType( PolyType.CHAR, 6 ), 1 ),
                new SpecialOperator( Kind.ARRAY_VALUE_CONSTRUCTOR.name(), Kind.ARRAY_VALUE_CONSTRUCTOR ),
                List.of( rexBuilder.makeLiteral( "status" ) ) );
        RexLiteral foldedPath = new RexLiteral(
                PolyList.of( PolyString.of( "status" ) ),
                typeFactory.createArrayType( typeFactory.createPolyType( PolyType.CHAR, 255 ), -1 ),
                PolyType.ARRAY );
        RexNode loweredMqlFilter = call(
                boolType,
                Kind.GREATER_THAN,
                new RexCall(
                        stringType,
                        new SpecialOperator( Kind.MQL_QUERY_VALUE.name(), Kind.MQL_QUERY_VALUE ),
                        List.of( RexIndexRef.of( 0, DocumentType.ofDoc() ), path ) ),
                new RexDynamicParam( stringType, 7 ) );
        RexNode castedConjunction = call(
                boolType,
                Kind.CAST,
                call(
                        boolType,
                        Kind.AND,
                        loweredMqlFilter,
                        call(
                                boolType,
                                Kind.LESS_THAN,
                                new RexCall(
                                        stringType,
                                        new SpecialOperator( Kind.MQL_QUERY_VALUE.name(), Kind.MQL_QUERY_VALUE ),
                        List.of( RexIndexRef.of( 0, DocumentType.ofDoc() ), path ) ),
                                new RexDynamicParam( stringType, 8 ) ) ) );
        RexNode foldedMqlFilter = call(
                boolType,
                Kind.LESS_THAN,
                new RexCall(
                        stringType,
                        new SpecialOperator( Kind.MQL_QUERY_VALUE.name(), Kind.MQL_QUERY_VALUE ),
                        List.of( RexIndexRef.of( 0, DocumentType.ofDoc() ), foldedPath ) ),
                new RexDynamicParam( stringType, 9 ) );

        assertNotNull( translated );
        assertEquals( 3, translated.columnIndex() );
        assertEquals( Kind.EQUALS, translated.operator() );
        assertEquals( 4L, translated.dynamicParamIndex() );
        assertNotNull( mqlGt );
        assertEquals( 3, mqlGt.columnIndex() );
        assertEquals( Kind.GREATER_THAN, mqlGt.operator() );
        assertEquals( 6L, mqlGt.dynamicParamIndex() );
        ParquetAdapterFilter<?> loweredMql = translator.translate( columns, loweredMqlFilter );
        assertNotNull( loweredMql );
        assertEquals( 3, loweredMql.columnIndex() );
        assertEquals( Kind.GREATER_THAN, loweredMql.operator() );
        assertEquals( 7L, loweredMql.dynamicParamIndex() );
        ParquetAdapterFilter<?> logical = translator.translate( columns, castedConjunction );
        assertNotNull( logical );
        assertEquals( Kind.AND, logical.operator() );
        assertEquals( 2, logical.operands().size() );
        assertEquals( Kind.GREATER_THAN, logical.operands().get( 0 ).operator() );
        assertEquals( Kind.LESS_THAN, logical.operands().get( 1 ).operator() );
        ParquetAdapterFilter<?> foldedMql = translator.translate( columns, foldedMqlFilter );
        assertNotNull( foldedMql );
        assertEquals( 3, foldedMql.columnIndex() );
        assertEquals( Kind.LESS_THAN, foldedMql.operator() );
        assertEquals( 9L, foldedMql.dynamicParamIndex() );
        assertNull( translator.translate( columns, nested ) );
    }


    @Test
    void docEnumeratorAppliesFiltersAndGeneratesIds() throws Exception {
        Path file = tempDir.resolve( "documents.parquet" );
        writeDocumentParquet( file );
        ParquetAdapterFilter<PolyValue> filter = new ParquetAdapterFilter<>( 0, Kind.EQUALS, PolyString.of( "Alice" ) );

        try ( ParquetDocEnumerator enumerator = new ParquetDocEnumerator(
                new ParquetSourceReader( Sources.of( file.toFile() ) ),
                FiltersContainer.shared( List.of( filter ) ) ) ) {
            assertTrue( enumerator.moveNext() );
            PolyDocument document = enumerator.current()[0].asDocument();
            assertEquals( "Alice", document.get( PolyString.of( "full_name" ) ).asString().value );
            assertTrue( document.get( PolyString.of( "_id" ) ).asString().value.endsWith( "documents.parquet#0" ) );
            assertFalse( enumerator.moveNext() );
        }
    }


    private static RexNode call( AlgDataType boolType, Kind kind, RexNode... operands ) {
        return new RexCall( boolType, new SpecialOperator( kind.name(), kind ), operands );
    }


    private static MessageType documentSchema() {
        return Types.buildMessage()
                .optional( PrimitiveTypeName.BINARY ).as( LogicalTypeAnnotation.stringType() ).named( "Full Name" )
                .optionalGroup()
                .optional( PrimitiveTypeName.BINARY ).as( LogicalTypeAnnotation.stringType() ).named( "Source Name" )
                .named( "Meta Data" )
                .repeated( PrimitiveTypeName.BINARY ).as( LogicalTypeAnnotation.stringType() ).named( "Tags" )
                .named( "doc_schema" );
    }


    private static void writeDocumentParquet( Path file ) throws Exception {
        MessageType schema = documentSchema();
        SimpleGroupFactory factory = new SimpleGroupFactory( schema );
        Group first = factory.newGroup()
                .append( "Full Name", "Alice" )
                .append( "Tags", "vip" );
        first.addGroup( "Meta Data" ).append( "Source Name", "crm" );
        Group second = factory.newGroup()
                .append( "Full Name", "Bob" )
                .append( "Tags", "south" );
        second.addGroup( "Meta Data" ).append( "Source Name", "erp" );

        try ( ParquetWriter<Group> writer = ExampleParquetWriter.builder( new LocalOutputFile( file ) )
                .withType( schema )
                .build() ) {
            writer.write( first );
            writer.write( second );
        }
    }


    private record LocalOutputFile( Path path ) implements OutputFile {

        @Override
        public PositionOutputStream create( long blockSizeHint ) throws IOException {
            return createOrOverwrite( blockSizeHint );
        }


        @SuppressWarnings("resource")
        @Override
        public PositionOutputStream createOrOverwrite( long blockSizeHint ) throws IOException {
            Files.createDirectories( path.getParent() );
            OutputStream outputStream = Files.newOutputStream( path );
            return new PositionOutputStream() {
                private long position;


                @Override
                public long getPos() {
                    return position;
                }


                @Override
                public void write( int b ) throws IOException {
                    outputStream.write( b );
                    position++;
                }


                @Override
                public void write( byte @NotNull [] b, int off, int len ) throws IOException {
                    outputStream.write( b, off, len );
                    position += len;
                }


                @Override
                public void close() throws IOException {
                    outputStream.close();
                }
            };
        }


        @Override
        public boolean supportsBlockSize() {
            return false;
        }


        @Override
        public long defaultBlockSize() {
            return 0;
        }

    }

}
