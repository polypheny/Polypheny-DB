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
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetBindingSerializer;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnBinding;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnRole;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetColumnStatistics;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetSourceFile;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetTableBinding;
import org.polypheny.db.adapter.parquet.shared.schema.ParquetMessageTypeBuilder;
import org.polypheny.db.adapter.parquet.shared.schema.ParquetNameNormalizer;
import org.polypheny.db.adapter.parquet.shared.schema.ParquetTypeConverter;
import org.polypheny.db.adapter.parquet.shared.schema.inference.FieldSchema;
import org.polypheny.db.adapter.parquet.shared.schema.inference.SchemaState;
import org.polypheny.db.adapter.parquet.shared.schema.inference.ValueKind;
import org.polypheny.db.adapter.parquet.shared.schema.inference.ValueSchema;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.numerical.PolyLong;
import org.polypheny.db.util.PolyphenyHomeDirManager;
import org.polypheny.db.util.RunMode;

class ParquetSchemaUtilitiesTest {

    @BeforeAll
    static void initHomeDir() {
        try {
            PolyphenyHomeDirManager.setModeAndGetInstance( RunMode.TEST );
        } catch ( Exception e ) {
            // Already initialized by another test.
        }
    }


    @Test
    void bindingSerializerRoundTripsSourceFilesPartitionsColumnsAndStatistics() {
        Map<List<String>, ParquetColumnStatistics> statistics = new LinkedHashMap<>();
        statistics.put( List.of( "Amount Total" ), new ParquetColumnStatistics( PolyType.DOUBLE, 3, 3, 1L, "10.5", "42.0", true ) );
        statistics.put( List.of( "Nested", "Count" ), new ParquetColumnStatistics( PolyType.INTEGER, 3, 2, null, "1", "8", false ) );

        ParquetSourceFile sourceFile = new ParquetSourceFile(
                "file:/tmp/orders|2026.parquet",
                Map.of( "region/name", "EMEA=North", "batch", "2026&06" ),
                statistics );
        Map<Long, ParquetColumnBinding> columns = new LinkedHashMap<>();
        columns.put( 7L, new ParquetColumnBinding( 7L, "Amount Total", ParquetColumnRole.DATA, List.of( "Amount Total" ) ) );
        columns.put( 8L, new ParquetColumnBinding( 8L, "region/name", ParquetColumnRole.PARTITION, List.of() ) );
        ParquetTableBinding binding = new ParquetTableBinding( List.of( sourceFile ), "Parent Table", List.of( "orders", "items" ), columns );

        Map<Long, ParquetTableBinding> bindings = new LinkedHashMap<>();
        bindings.put( 99L, binding );

        Map<Long, ParquetTableBinding> restored = ParquetBindingSerializer.deserialize( ParquetBindingSerializer.serialize( bindings ) );

        assertEquals( binding, restored.get( 99L ) );
    }


    @Test
    void nameNormalizerNormalizesAndUniquifiesNestedFields() {
        ValueSchema nested = ValueSchema.groupType();
        nested.nested().add( new FieldSchema( "Line #", "", -1, false, ValueSchema.stringType() ) );
        nested.nested().add( new FieldSchema( "Line !", "", -1, false, ValueSchema.int32Type() ) );
        List<FieldSchema> fields = new ArrayList<>( List.of(
                new FieldSchema( "Customer Name", "", 0, false, ValueSchema.stringType() ),
                new FieldSchema( "customer-name", "", 1, false, ValueSchema.stringType() ),
                new FieldSchema( "Items", "", 2, false, nested ) ) );

        ParquetNameNormalizer.uniquifyParquetFieldNames( fields );

        assertEquals( "ordersdb", ParquetNameNormalizer.computePhysicalTableName( "Orders DB.parquet" ) );
        assertEquals( "customer_name", fields.get( 0 ).getParquetName() );
        assertEquals( "customer_name_2", fields.get( 1 ).getParquetName() );
        assertEquals( "items", fields.get( 2 ).getParquetName() );
        assertEquals( "line_", nested.nested().get( 0 ).getParquetName() );
        assertEquals( "line__2", nested.nested().get( 1 ).getParquetName() );
    }


    @Test
    void typeConverterConvertsTypedStringsAndComparesNumerically() {
        ParquetTypeConverter converter = new ParquetTypeConverter();

        assertEquals( 7, converter.fromStringToPolyValue( PolyType.INTEGER, "7" ).asNumber().intValue() );
        assertEquals( "hello", converter.fromStringToPolyValue( PolyType.TEXT, "hello" ).asString().value );
        assertNotNull( converter.fromStringToCompatiblePolyValue( PolyType.BIGINT, PolyType.INTEGER, "42" ) );
        assertNull( converter.fromStringToCompatiblePolyValue( PolyType.BOOLEAN, PolyType.INTEGER, "42" ) );
        assertEquals( 1, converter.compareStringValues( PolyType.INTEGER, "10", "2" ) );
    }


    @Test
    void schemaStateSkipsPrimaryKeyAndStringifiesConflictingSampleValues() {
        AlgDataType inputType = AlgDataTypeFactory.DEFAULT.builder()
                .add( "_key", null, PolyType.BIGINT )
                .add( "Mixed Value", null, PolyType.INTEGER )
                .build();
        SchemaState schemaState = new SchemaState( SchemaState.CONFLICT_STRINGIFY );
        schemaState.init( inputType, false );

        assertEquals( 1, schemaState.getFields().size() );
        assertEquals( 1, schemaState.getFields().get( 0 ).getSourceIndex() );
        assertEquals( "mixed_value", schemaState.getFields().get( 0 ).getParquetName() );

        schemaState.mergeRelationalRowSchema( List.of( PolyLong.of( 1 ), PolyString.of( "not-an-int" ) ), inputType, false );

        assertEquals( ValueKind.STRING, schemaState.getFields().get( 0 ).getValueSchema().kind() );
    }


    @Test
    void valueSchemaMergesNestedFieldsAndWidensNumericTypes() {
        assertEquals( ValueKind.DOUBLE, ValueSchema.int32Type().mergeValueSchemas( ValueSchema.doubleType(), SchemaState.CONFLICT_FAIL ).kind() );

        ValueSchema left = ValueSchema.groupType();
        left.mergeNested( "count", ValueSchema.int32Type(), SchemaState.CONFLICT_FAIL );
        ValueSchema right = ValueSchema.groupType();
        right.mergeNested( "count", ValueSchema.int64Type(), SchemaState.CONFLICT_FAIL );
        right.mergeNested( "label", ValueSchema.stringType(), SchemaState.CONFLICT_FAIL );

        ValueSchema merged = left.mergeValueSchemas( right, SchemaState.CONFLICT_FAIL );

        assertEquals( ValueKind.GROUP, merged.kind() );
        assertEquals( 2, merged.nested().size() );
        assertEquals( ValueKind.INT64, merged.nested().get( 0 ).getValueSchema().kind() );
        assertEquals( "label", merged.nested().get( 1 ).getParquetName() );
    }


    @Test
    void messageTypeBuilderBuildsRepeatedAndNestedFields() {
        SchemaState schemaState = new SchemaState( SchemaState.CONFLICT_FAIL );
        ValueSchema details = ValueSchema.groupType();
        details.nested().add( new FieldSchema( "Name", "name", -1, false, ValueSchema.stringType() ) );
        schemaState.addField( new FieldSchema( "Tags", "tags", -1, true, ValueSchema.stringType() ) );
        schemaState.addField( new FieldSchema( "Details", "details", -1, false, details ) );

        MessageType messageType = new ParquetMessageTypeBuilder( schemaState, "test_schema" ).build();

        assertEquals( Type.Repetition.REPEATED, messageType.getType( "tags" ).getRepetition() );
        assertFalse( messageType.getType( "details" ).isPrimitive() );
        assertEquals( PrimitiveTypeName.BINARY, messageType.getType( "details" ).asGroupType().getType( "name" ).asPrimitiveType().getPrimitiveTypeName() );
        assertThrows( GenericRuntimeException.class, () -> new ParquetMessageTypeBuilder( new SchemaState( SchemaState.CONFLICT_FAIL ), "empty" ).build() );
    }

}
