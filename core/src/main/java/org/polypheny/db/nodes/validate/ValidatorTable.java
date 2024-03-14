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

package org.polypheny.db.nodes.validate;


import java.util.List;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.constant.Monotonicity;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.util.Wrapper;


/**
 * Supplies a {@link Validator} with the metadata for a table.
 */
public interface ValidatorTable extends Wrapper {


    AlgDataType getTupleType();

    List<String> getQualifiedName();

    /**
     * Returns whether a given column is monotonic.
     */
    Monotonicity getMonotonicity( String columnName );

    @Deprecated
    static Monotonicity getMonotonicity( LogicalTable table, String columnName ) {
        for ( AlgCollation collation : table.getStatistic().getCollations() ) {
            final AlgFieldCollation fieldCollation = collation.getFieldCollations().get( 0 );
            final int fieldIndex = fieldCollation.getFieldIndex();
            if ( fieldIndex < table.getTupleType().getFieldCount() && table.getTupleType().getFieldNames().get( fieldIndex ).equals( columnName ) ) {
                return fieldCollation.direction.monotonicity();
            }
        }
        return Monotonicity.NOT_MONOTONIC;
    }

}

