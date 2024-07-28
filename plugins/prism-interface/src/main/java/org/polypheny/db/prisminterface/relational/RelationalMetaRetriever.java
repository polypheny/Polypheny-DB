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
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgOptUtil;
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

    private static final int ORIGIN_COLUMN_INDEX = 3;
    private static final int ORIGIN_TABLE_INDEX = 2;
    private static final int ORIGIN_SCHEMA_INDEX = 1;


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
        AlgDataType algDataType = retrieveAlgDataType( polyImplementation );
        AlgDataType whatever = QueryProcessorHelpers.makeStruct( polyImplementation.getStatement().getTransaction().getTypeFactory(), algDataType );
        List<List<String>> origins = polyImplementation.getPreparedResult().getFieldOrigins();
        List<ColumnMeta> columns = new ArrayList<>();
        int index = 0;
        for ( Ord<AlgDataTypeField> pair : Ord.zip( whatever.getFields() ) ) {
            final AlgDataTypeField field = pair.e;
            final AlgDataType type = field.getType();
            columns.add( retrieveColumnMeta( index++, field.getName(), type, origins.get( pair.i ) ) );
        }
        return columns;
    }


    private static ProtoPolyType getFromPolyType( PolyType polyType ) {
        return ProtoPolyType.valueOf( polyType.getName() );
    }


    private static ColumnMeta retrieveColumnMeta(
            int index,
            String fieldName,
            AlgDataType type,
            List<String> origins ) {
        TypeMeta typeMeta = retrieveTypeMeta( type );
        ColumnMeta.Builder columnMetaBuilder = ColumnMeta.newBuilder();
        Optional.ofNullable( QueryProcessorHelpers.origin( origins, ORIGIN_TABLE_INDEX ) ).ifPresent( columnMetaBuilder::setEntityName );
        Optional.ofNullable( QueryProcessorHelpers.origin( origins, ORIGIN_SCHEMA_INDEX ) ).ifPresent( columnMetaBuilder::setSchemaName );
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
                return polyImplementation.tupleType;
        }
    }

}
