/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.protointerface.relational;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.Ord;
import org.checkerframework.checker.units.qual.A;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.processing.QueryProcessorHelpers;
import org.polypheny.db.protointerface.proto.ArrayMeta;
import org.polypheny.db.protointerface.proto.ColumnMeta;
import org.polypheny.db.protointerface.proto.FieldMeta;
import org.polypheny.db.protointerface.proto.ParameterMeta;
import org.polypheny.db.protointerface.proto.ProtoValueType;
import org.polypheny.db.protointerface.proto.StructMeta;
import org.polypheny.db.protointerface.proto.TypeMeta;
import org.polypheny.db.type.PolyType;

public class RelationalMetaRetriever {

    private static final String PROTO_VALUE_TYPE_PREFIX = "PROTO_VALUE_TYPE_";
    private static final int ORIGIN_COLUMN_INDEX = 3;
    private static final int ORIGIN_TABLE_INDEX = 2;
    private static final int ORIGIN_SCHEMA_INDEX = 1;
    private static final int ORIGIN_DATABASE_INDEX = 0;


    public static List<ParameterMeta> retrieveParameterMetas( AlgDataType parameterRowType, ImmutableBiMap<String, Integer> namedIndexes) {
        int index = 0;
        return parameterRowType.getFieldList().stream()
                .map( p -> retrieveParameterMeta(p, namedIndexes.inverse().get( index )) )
                .collect( Collectors.toList() );
    }


    private static ParameterMeta retrieveParameterMeta( AlgDataTypeField algDataTypeField, String parameterName ) {
        AlgDataType algDataType = algDataTypeField.getType();
        return ParameterMeta.newBuilder()
                .setName( algDataTypeField.getName() )
                .setTypeName( algDataType.getPolyType().getTypeName())
                .setPrecision( QueryProcessorHelpers.getPrecision( algDataType ) )
                .setScale( QueryProcessorHelpers.getScale( algDataType ) )
                .setParameterName( parameterName )
                .build();
    }



    public static List<ColumnMeta> retrieveColumnMetas( PolyImplementation polyImplementation ) {
        AlgDataType algDataType = retrieveAlgDataType( polyImplementation );
        AlgDataType whatever = QueryProcessorHelpers.makeStruct( polyImplementation.getStatement().getTransaction().getTypeFactory(), algDataType );
        List<List<String>> origins = (List<List<String>>) polyImplementation.getPreparedResult().getFieldOrigins();
        List<ColumnMeta> columns = new ArrayList<>();
        int index = 0;
        for ( Ord<AlgDataTypeField> pair : Ord.zip( whatever.getFieldList() ) ) {
            final AlgDataTypeField field = pair.e;
            final AlgDataType type = field.getType();
            columns.add( retrieveColumnMeta( index++, field.getName(), type, origins.get( pair.i ) ) );
        }
        return columns;
    }


    private static ProtoValueType getFromPolyType( PolyType polyType ) {
        return ProtoValueType.valueOf( PROTO_VALUE_TYPE_PREFIX + polyType.getName() );
    }


    private static ColumnMeta retrieveColumnMeta(
            int index,
            String fieldName,
            AlgDataType type,
            List<String> origins ) {
        TypeMeta typeMeta = retrieveTypeMeta( type );
        ColumnMeta.Builder columnMetaBuilder = ColumnMeta.newBuilder();
        applyIfNotNull( columnMetaBuilder::setEntityName, QueryProcessorHelpers.origin( origins, ORIGIN_TABLE_INDEX ) );
        applyIfNotNull( columnMetaBuilder::setSchemaName, QueryProcessorHelpers.origin( origins, ORIGIN_SCHEMA_INDEX ) );
        return columnMetaBuilder
                .setColumnIndex( index )
                .setColumnName( fieldName ) // designated column name
                .setColumnAlias( getColumnLabel( origins, type, fieldName ) ) // alias as specified in sql AS clause
                .setIsNullable( type.isNullable() )
                .setLength( type.getPrecision() )
                .setPrecision( QueryProcessorHelpers.getPrecision( type ) ) // <- same as type.getPrecision() but returns 0 if not applicable
                .setScale( type.getScale() )
                .setTypeMeta( typeMeta )
                //.setNamespace()
                //TODO TH: find out how to get namespace form here
                .build();
    }


    private static <R> R applyIfNotNull( Function<String, R> function, String value ) {
        if ( value == null ) {
            return null;
        }
        return function.apply( value );
    }


    private static String getColumnLabel( List<String> origins, AlgDataType type, String fieldName ) {
        String columnLabel = QueryProcessorHelpers.origin( origins, ORIGIN_COLUMN_INDEX );
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
                    .setArrayMeta( arrayMeta )
                    .setProtoValueType( ProtoValueType.PROTO_VALUE_TYPE_ARRAY )
                    .build();
        } else {
            PolyType polyType = algDataType.getPolyType();
            if ( Objects.requireNonNull( polyType ) == PolyType.STRUCTURED ) {
                List<FieldMeta> fieldMetas = algDataType
                        .getFieldList()
                        .stream()
                        .map( f -> retrieveFieldMeta( f.getIndex(), f.getName(), f.getType() ) )
                        .collect( Collectors.toList() );
                return TypeMeta.newBuilder()
                        .setStructMeta( StructMeta.newBuilder().addAllFieldMetas( fieldMetas ).build() )
                        .setProtoValueType( ProtoValueType.PROTO_VALUE_TYPE_STRUCTURED )
                        .build();
            }
            return TypeMeta.newBuilder()
                    .setProtoValueType( getFromPolyType( polyType ) )
                    .build();
        }
    }


    private static AlgDataType retrieveAlgDataType( PolyImplementation polyImplementation ) {
        switch ( polyImplementation.getKind() ) {
            case INSERT:
            case DELETE:
            case UPDATE:
            case EXPLAIN:
                // FIXME: getValidatedNodeType is wrong for DML
                Kind kind = polyImplementation.getKind();
                JavaTypeFactory typeFactory = polyImplementation.getStatement().getTransaction().getTypeFactory();
                return AlgOptUtil.createDmlRowType( kind, typeFactory );
            default:
                return polyImplementation.getRowType();
        }
    }

}
