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

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.linq4j.Ord;
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


    public static List<ColumnMetaData> getColumnMetaDataList( AlgDataType jdbcType, List<List<String>> originList ) {
        final List<ColumnMetaData> columns = new ArrayList<>();
        for ( Ord<AlgDataTypeField> pair : Ord.zip( jdbcType.getFields() ) ) {
            final AlgDataTypeField field = pair.e;
            columns.add( QueryProcessorHelpers.metaData( field.getName(), originList.get( pair.i ) ) );
        }
        return columns;
    }


    public static ColumnMetaData metaData(
            String fieldName,
            List<String> origins ) {
        return new ColumnMetaData(
                fieldName,
                QueryProcessorHelpers.origin( origins, 0 )
        );
    }

}
