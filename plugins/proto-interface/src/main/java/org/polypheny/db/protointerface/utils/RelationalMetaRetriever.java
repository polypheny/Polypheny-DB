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

package org.polypheny.db.protointerface.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.Ord;
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
import org.polypheny.db.protointerface.proto.ProtoValueType;
import org.polypheny.db.protointerface.proto.StructMeta;
import org.polypheny.db.protointerface.proto.TypeMeta;
import org.polypheny.db.type.PolyType;

public class RelationalMetaRetriever {

    private static final String PROTO_VALUE_TYPE_PREFIX = "PROTO_VALUE_TYPE_";
    private static final int ORIGIN_COLUMN_NAME_OFFSET = 0;
    private static final int ORIGIN_TABLE_NAME_OFFSET = 1;
    private static final int ORIGIN_SCHEMA_NAME_OFFSET = 2;


    public static List<ColumnMeta> retrieveColumnMetas( PolyImplementation polyImplementation ) {
        AlgDataType algDataType = retrieveAlgDataType( polyImplementation );
        AlgDataType whatever = QueryProcessorHelpers.makeStruct( polyImplementation.getStatement().getPrepareContext().getTypeFactory(), algDataType );
        List<List<String>> origins = polyImplementation.getPreparedResult().getFieldOrigins();

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
        return ColumnMeta.newBuilder()
                .setColumnIndex( index )
                .setColumnLabel( fieldName )
                .setIsNullable( type.isNullable() )
                .setLength( type.getPrecision() )
                .setPrecision( QueryProcessorHelpers.getPrecision( type ) ) // <- same as type.getPrecision() but returns 0 if not applicable
                .setColumnName( QueryProcessorHelpers.origin( origins, ORIGIN_COLUMN_NAME_OFFSET ) )
                .setTableName( QueryProcessorHelpers.origin( origins, ORIGIN_TABLE_NAME_OFFSET ) )
                .setSchemaName( QueryProcessorHelpers.origin( origins, ORIGIN_SCHEMA_NAME_OFFSET ) )
                .setScale( type.getScale() )
                .setTypeMeta( typeMeta )
                //.setNamespace()
                //TODO TH: find out how to get namespace form here
                .build();
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
                ;
                return AlgOptUtil.createDmlRowType( kind, typeFactory );
            default:
                return polyImplementation.getRowType();
        }
    }
}
