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

package org.polypheny.db.adapter.parquet.relational.planning;

import java.util.Arrays;
import java.util.List;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetRelTable;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataTypeField;

final class ParquetPolyAlgDisplay {

    private ParquetPolyAlgDisplay() {
    }


    static List<String> fieldNames( ParquetRelTable table, int[] fields ) {
        List<AlgDataTypeField> tableFields = table.getTupleType().getFields();
        return Arrays.stream( fields )
                .mapToObj( field -> fieldName( tableFields, field ) )
                .toList();
    }


    static List<String> filters( List<ParquetAdapterFilter> filters, List<String> fieldNames ) {
        return filters.stream()
                .map( filter -> filter( filter, fieldNames ) )
                .toList();
    }


    static String filter( ParquetAdapterFilter filter, List<String> fieldNames ) {
        if ( filter.isLogical() ) {
            return filter.operator().name() + "("
                    + String.join( ", ", filter.operands().stream()
                            .map( operand -> filter( operand, fieldNames ) )
                            .toList() )
                    + ")";
        }

        String field = fieldRef( fieldNames, filter.columnIndex() );
        if ( filter.operator() == Kind.IS_NULL || filter.operator() == Kind.IS_NOT_NULL ) {
            return field + " " + operator( filter );
        }

        String value;
        if ( filter.dynamicParamIndex() != null ) {
            value = "?" + filter.dynamicParamIndex();
        } else if ( filter.polyValue() == null ) {
            value = "null";
        } else {
            value = filter.polyValue().toString();
        }

        return field + " " + operator( filter ) + " " + value;
    }


    static List<String> joinedFieldNames( ParquetRelTable leftTable, int[] leftFields, ParquetRelTable rightTable, int[] rightFields ) {
        List<String> leftNames = fieldNames( leftTable, leftFields ).stream()
                .map( name -> "left." + name )
                .toList();
        List<String> rightNames = fieldNames( rightTable, rightFields ).stream()
                .map( name -> "right." + name )
                .toList();
        return java.util.stream.Stream.concat( leftNames.stream(), rightNames.stream() ).toList();
    }


    private static String fieldRef( List<String> fieldNames, int index ) {
        if ( index >= 0 && index < fieldNames.size() ) {
            return fieldNames.get( index );
        }
        return "#" + index;
    }


    private static String operator( ParquetAdapterFilter filter ) {
        return filter.operator().sql == null ? filter.operator().name() : filter.operator().sql;
    }


    private static String fieldName( List<AlgDataTypeField> fields, int index ) {
        if ( index >= 0 && index < fields.size() ) {
            return fields.get( index ).getName() + " (#" + index + ")";
        }
        return "#" + index;
    }

}
