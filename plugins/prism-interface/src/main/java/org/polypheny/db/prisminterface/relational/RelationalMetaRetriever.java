/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.prisminterface.relational;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.processing.QueryProcessorHelpers;
import org.polypheny.db.type.PolyType;
import org.polypheny.prism.ArrayMeta;
import org.polypheny.prism.ColumnMeta;
import org.polypheny.prism.FieldMeta;
import org.polypheny.prism.ParameterMeta;
import org.polypheny.prism.ProtoPolyType;
import org.polypheny.prism.StructMeta;
import org.polypheny.prism.TypeMeta;

public class RelationalMetaRetriever {

    private static final int ORIGIN_COLUMN_OFFSET = 0;
    private static final int ORIGIN_TABLE_OFFSET = 1;
    private static final int ORIGIN_SCHEMA_OFFSET = 2;


    public static List<ParameterMeta> retrieveParameterMetas( AlgDataType parameterRowType ) {
        return parameterRowType.getFields().stream()
                .map( p -> retrieveParameterMeta( p, null ) )
                .toList();
    }


    private static ParameterMeta retrieveParameterMeta( AlgDataTypeField algDataTypeField, String parameterName ) {
        AlgDataType algDataType = algDataTypeField.getType();
        ParameterMeta.Builder metaBuilder = ParameterMeta.newBuilder();
        metaBuilder.setName( algDataTypeField.getName() );
        metaBuilder.setTypeName( algDataType.getPolyType().getTypeName() );
        metaBuilder.setPrecision( QueryProcessorHelpers.getPrecision( algDataType ) );
        metaBuilder.setScale( QueryProcessorHelpers.getScale( algDataType ) );
        Optional.ofNullable( parameterName ).ifPresent( p -> metaBuilder.setParameterName( parameterName ) );
        return metaBuilder.build();
    }


    public static List<ColumnMeta> retrieveColumnMetas( PolyImplementation polyImplementation ) {
        AlgDataType algDataType = polyImplementation.getResultRowType();
        if ( algDataType.getPolyType() == PolyType.GRAPH ) {
            // GraphType's storage fields describe its ID, but a full scan returns one graph value.
            return List.of( retrieveColumnMeta( 0, "graph", algDataType, null ) );
        }
        AlgDataType whatever = QueryProcessorHelpers.makeStruct( polyImplementation.getStatement().getTransaction().getTypeFactory(), algDataType );
        return retrieveColumnMetas( whatever, polyImplementation.getFieldOrigins(), polyImplementation.getStatement().getTransaction().getSnapshot(), polyImplementation.getIdentityFields() );
    }


    public static List<ColumnMeta> retrieveColumnMetas( AlgDataType rowType, List<List<String>> origins, Snapshot snapshot ) {
        return retrieveColumnMetas( rowType, origins, snapshot, List.of() );
    }


    private static List<ColumnMeta> retrieveColumnMetas( AlgDataType rowType, List<List<String>> origins, Snapshot snapshot, List<Boolean> identityFields ) {
        List<ColumnMeta> columns = new ArrayList<>();
        for ( Ord<AlgDataTypeField> pair : Ord.zip( rowType.getFields() ) ) {
            AlgDataTypeField field = pair.e;
            List<String> origin = origins.get( pair.i );
            ColumnMeta meta = retrieveColumnMeta( pair.i, field.getName(), field.getType(), origin );
            boolean identity = pair.i < identityFields.size() ? identityFields.get( pair.i ) : isIdentityColumn( origin, snapshot );
            columns.add( meta.toBuilder().setIsAutoIncrement( identity ).build() );
        }
        return columns;
    }


    private static boolean isIdentityColumn( List<String> origins, Snapshot snapshot ) {
        String namespace = QueryProcessorHelpers.origin( origins, ORIGIN_SCHEMA_OFFSET );
        String table = QueryProcessorHelpers.origin( origins, ORIGIN_TABLE_OFFSET );
        String column = QueryProcessorHelpers.origin( origins, ORIGIN_COLUMN_OFFSET );
        if ( namespace == null || table == null || column == null ) {
            return false;
        }
        return snapshot.getNamespace( namespace )
                .flatMap( schema -> snapshot.rel().getColumn( schema.id, table, column ) )
                .filter( logical -> logical.getIdentity() != null )
                .isPresent();
    }


    private static ProtoPolyType getFromPolyType( PolyType polyType ) {
        if ( polyType == PolyType.CYPHER_TEMPORAL ) {
            return ProtoPolyType.VARCHAR;
        }
        return ProtoPolyType.valueOf( polyType.getName() );
    }


    private static ColumnMeta retrieveColumnMeta(
            int index,
            String fieldName,
            AlgDataType type,
            List<String> origins ) {
        TypeMeta typeMeta = retrieveTypeMeta( type );
        ColumnMeta.Builder columnMetaBuilder = ColumnMeta.newBuilder();
        Optional.ofNullable( QueryProcessorHelpers.origin( origins, ORIGIN_TABLE_OFFSET ) ).ifPresent( columnMetaBuilder::setEntityName );
        Optional.ofNullable( QueryProcessorHelpers.origin( origins, ORIGIN_SCHEMA_OFFSET ) ).ifPresent( columnMetaBuilder::setSchemaName );
        return columnMetaBuilder
                .setColumnIndex( index )
                .setColumnName( fieldName ) // designated column name
                .setColumnLabel( getColumnLabel( origins, fieldName ) ) // alias as specified in sql AS clause
                .setIsNullable( type.isNullable() )
                .setLength( type.getPrecision() )
                .setPrecision( QueryProcessorHelpers.getPrecision( type ) ) // <- same as type.getPrecision() but returns 0 if not applicable
                .setScale( type.getScale() )
                .setTypeMeta( typeMeta )
                //.setNamespace()
                //TODO: find out how to get namespace form here
                .build();
    }


    private static String getColumnLabel( List<String> origins, String fieldName ) {
        String columnLabel = QueryProcessorHelpers.origin( origins, ORIGIN_COLUMN_OFFSET );
        return columnLabel == null ? fieldName : columnLabel;
    }


    private static FieldMeta retrieveFieldMeta(
            int index,
            String fieldName,
            AlgDataType type ) {
        TypeMeta typeMeta = retrieveTypeMeta( type );
        return FieldMeta.newBuilder()
                .setFieldIndex( index )
                .setFieldName( fieldName )
                .setIsNullable( type.isNullable() )
                .setLength( type.getPrecision() )
                .setPrecision( QueryProcessorHelpers.getPrecision( type ) ) // <- same as type.getPrecision() but returns 0 if not applicable
                .setScale( type.getScale() )
                .setTypeMeta( typeMeta )
                .build();
    }


    private static TypeMeta retrieveTypeMeta( AlgDataType algDataType ) {
        AlgDataType componentType = algDataType.getComponentType();
        // type is an array
        if ( componentType != null ) {
            TypeMeta elementTypeMeta = retrieveTypeMeta( componentType );
            ArrayMeta arrayMeta = ArrayMeta.newBuilder()
                    .setElementType( elementTypeMeta )
                    .build();
            return TypeMeta.newBuilder()
                    .setProtoValueType( ProtoPolyType.ARRAY )
                    .setArrayMeta( arrayMeta )
                    .build();
        } else {
            PolyType polyType = algDataType.getPolyType();
            if ( Objects.requireNonNull( polyType ) == PolyType.STRUCTURED ) {
                List<FieldMeta> fieldMetas = algDataType
                        .getFields()
                        .stream()
                        .map( f -> retrieveFieldMeta( f.getIndex(), f.getName(), f.getType() ) )
                        .toList();
                return TypeMeta.newBuilder()
                        .setStructMeta( StructMeta.newBuilder().addAllFieldMetas( fieldMetas ).build() )
                        //.setProtoValueType( ProtoValueType.PROTO_VALUE_TYPE_STRUCTURED )
                        //TODO TH: handle structured type meta in a useful way
                        .build();
            }
            ProtoPolyType type = getFromPolyType( polyType );
            return TypeMeta.newBuilder()
                    .setProtoValueType( type )
                    .build();
        }
    }


}
