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

package org.polypheny.db.processing;

import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.common.Modify;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.util.avatica.ColumnMetaData;


/**
 * Some extracted helper function from AbstractQueryProcessor.
 */
public class QueryProcessorHelpers {

    public static AlgDataType makeStruct( AlgDataTypeFactory typeFactory, AlgDataType type ) {
        if ( type.isStruct() ) {
            return type;
        }
        // TODO MV: This "null" might be wrong
        return typeFactory.builder().add( null, "$0", null, type ).build();
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


    public static int getScale( AlgDataType type ) {
        return type.getScale() == AlgDataType.SCALE_NOT_SPECIFIED
                ? 0
                : type.getScale();
    }


    public static int getTypeOrdinal( AlgDataType type ) {
        return type.getPolyType().getJdbcOrdinal();
    }


    public static LogicalRelModify.Operation mapTableModOp( boolean isDml, Kind sqlKind ) {
        if ( !isDml ) {
            return null;
        }
        return switch ( sqlKind ) {
            case INSERT -> Modify.Operation.INSERT;
            case DELETE -> Modify.Operation.DELETE;
            case MERGE -> Modify.Operation.MERGE;
            case UPDATE -> Modify.Operation.UPDATE;
            default -> null;
        };
    }


    public static List<ColumnMetaData> getColumnMetaDataList( JavaTypeFactory typeFactory, AlgDataType x, AlgDataType jdbcType, List<List<String>> originList ) {
        final List<ColumnMetaData> columns = new ArrayList<>();
        for ( Ord<AlgDataTypeField> pair : Ord.zip( jdbcType.getFields() ) ) {
            final AlgDataTypeField field = pair.e;
            final AlgDataType type = field.getType();
            final AlgDataType fieldType = x.isStruct() ? x.getFields().get( pair.i ).getType() : type;
            columns.add( QueryProcessorHelpers.metaData( typeFactory, columns.size(), field.getName(), type, fieldType, originList.get( pair.i ) ) );
        }
        return columns;
    }


    public static ColumnMetaData metaData(
            JavaTypeFactory typeFactory,
            int ordinal,
            String fieldName,
            AlgDataType type,
            AlgDataType fieldType,
            List<String> origins ) {
        return new ColumnMetaData(
                ordinal, //XXX ordinal
                false, // auto inc
                true, //case sensitive
                false, //searchable
                false, // currency
                type.isNullable() //XXX nullable
                        ? DatabaseMetaData.columnNullable
                        : DatabaseMetaData.columnNoNulls,
                true, //signed
                type.getPrecision(), //XXX display size
                fieldName, //XXX label
                QueryProcessorHelpers.origin( origins, 0 ), //XXX column name
                QueryProcessorHelpers.origin( origins, 2 ), //XXX schema name
                QueryProcessorHelpers.getPrecision( type ), //XXX precision
                0, // XXX scale; This is a workaround for a bug in Avatica with Decimals. There is no need to change the scale //getScale( type ),
                QueryProcessorHelpers.origin( origins, 1 ), //XXX table name
                null, //XXXcatalog name = namespace
                fieldType.getPolyType(), //type
                true, // read only
                false, // writable
                false // definitely writable
        );
    }

}
