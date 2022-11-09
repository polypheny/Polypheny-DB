/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.processing;

import java.lang.reflect.Type;
import java.sql.DatabaseMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.avatica.ColumnMetaData;
import org.apache.calcite.avatica.ColumnMetaData.Rep;
import org.apache.calcite.avatica.Meta.StatementType;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.logical.relational.LogicalModify;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.prepare.Prepare.PreparedResult;
import org.polypheny.db.type.ArrayType;
import org.polypheny.db.type.ExtraPolyTypes;
import org.polypheny.db.util.Util;


/**
 * Some extracted helper function from AbstractQueryProcessor.
 */
public class QueryProcessorHelpers {

    public static AlgDataType makeStruct( AlgDataTypeFactory typeFactory, AlgDataType type ) {
        if ( type.isStruct() ) {
            return type;
        }
        // TODO MV: This "null" might be wrong
        return typeFactory.builder().add( "$0", null, type ).build();
    }


    public static String origin( List<String> origins, int offsetFromEnd ) {
        return origins == null || offsetFromEnd >= origins.size()
                ? null
                : origins.get( origins.size() - 1 - offsetFromEnd );
    }


    public static int getPrecision( AlgDataType type ) {
        return type.getPrecision() == AlgDataType.PRECISION_NOT_SPECIFIED
                ? 0
                : type.getPrecision();
    }


    public static int getTypeOrdinal( AlgDataType type ) {
        return type.getPolyType().getJdbcOrdinal();
    }


    public static StatementType getStatementType( PreparedResult preparedResult ) {
        if ( preparedResult.isDml() ) {
            return StatementType.IS_DML;
        } else {
            return StatementType.SELECT;
        }
    }


    public static LogicalModify.Operation mapTableModOp( boolean isDml, Kind sqlKind ) {
        if ( !isDml ) {
            return null;
        }
        switch ( sqlKind ) {
            case INSERT:
                return LogicalModify.Operation.INSERT;
            case DELETE:
                return LogicalModify.Operation.DELETE;
            case MERGE:
                return LogicalModify.Operation.MERGE;
            case UPDATE:
                return LogicalModify.Operation.UPDATE;
            default:
                return null;
        }
    }


    public static List<ColumnMetaData> getColumnMetaDataList( JavaTypeFactory typeFactory, AlgDataType x, AlgDataType jdbcType, List<List<String>> originList ) {
        final List<ColumnMetaData> columns = new ArrayList<>();
        for ( Ord<AlgDataTypeField> pair : Ord.zip( jdbcType.getFieldList() ) ) {
            final AlgDataTypeField field = pair.e;
            final AlgDataType type = field.getType();
            final AlgDataType fieldType = x.isStruct() ? x.getFieldList().get( pair.i ).getType() : type;
            columns.add( QueryProcessorHelpers.metaData( typeFactory, columns.size(), field.getName(), type, fieldType, originList.get( pair.i ) ) );
        }
        return columns;
    }


    public static ColumnMetaData.AvaticaType avaticaType( JavaTypeFactory typeFactory, AlgDataType type, AlgDataType fieldType ) {
        final String typeName = type.getPolyType().getTypeName();
        if ( type.getComponentType() != null ) {
            final ColumnMetaData.AvaticaType componentType = avaticaType( typeFactory, type.getComponentType(), null );
            final ColumnMetaData.Rep rep = Rep.ARRAY;
            return ColumnMetaData.array( componentType, typeName, rep );
        } else {
            int typeOrdinal = QueryProcessorHelpers.getTypeOrdinal( type );
            switch ( typeOrdinal ) {
                case Types.STRUCT:
                    final List<ColumnMetaData> columns = new ArrayList<>();
                    for ( AlgDataTypeField field : type.getFieldList() ) {
                        columns.add( metaData( typeFactory, field.getIndex(), field.getName(), field.getType(), null, null ) );
                    }
                    return ColumnMetaData.struct( columns );
                case ExtraPolyTypes.GEOMETRY:
                    typeOrdinal = Types.VARCHAR;
                    // Fall through
                default:
                    final Type clazz = typeFactory.getJavaClass( Util.first( fieldType, type ) );
                    final ColumnMetaData.Rep rep = ColumnMetaData.Rep.of( clazz );
                    assert rep != null;
                    return ColumnMetaData.scalar( typeOrdinal, typeName, rep );
            }
        }
    }


    public static ColumnMetaData metaData(
            JavaTypeFactory typeFactory,
            int ordinal,
            String fieldName,
            AlgDataType type,
            AlgDataType fieldType,
            List<String> origins ) {
        final ColumnMetaData.AvaticaType avaticaType = avaticaType( typeFactory, type, fieldType );
        return new ColumnMetaData(
                ordinal,
                false,
                true,
                false,
                false,
                type.isNullable()
                        ? DatabaseMetaData.columnNullable
                        : DatabaseMetaData.columnNoNulls,
                true,
                type.getPrecision(),
                fieldName,
                QueryProcessorHelpers.origin( origins, 0 ),
                QueryProcessorHelpers.origin( origins, 2 ),
                QueryProcessorHelpers.getPrecision( type ),
                0, // This is a workaround for a bug in Avatica with Decimals. There is no need to change the scale //getScale( type ),
                QueryProcessorHelpers.origin( origins, 1 ),
                null,
                avaticaType,
                true,
                false,
                false,
//                avaticaType.columnClassName() );
                (fieldType instanceof ArrayType) ? "java.util.List" : avaticaType.columnClassName() );
    }

}
