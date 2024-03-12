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

package org.polypheny.db.restapi;


import java.util.Objects;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.catalog.entity.logical.LogicalColumn;


public class RequestColumn {

    @Getter
    private final LogicalColumn column;
    private final int tableScanIndex;
    @Setter
    @Getter
    private int logicalIndex;
    @Getter
    private final String fullyQualifiedName;
    @Getter
    private final String alias;
    @Getter
    private final AggFunction aggregate;
    @Getter
    private final boolean explicit;


    RequestColumn( LogicalColumn column, int tableScanIndex, int logicalIndex, String alias, AggFunction aggregate, boolean explicit ) {
        this.column = Objects.requireNonNull( column );
        this.tableScanIndex = tableScanIndex;
        this.logicalIndex = logicalIndex;
        this.fullyQualifiedName = column.getNamespaceName() + "." + column.getTableName() + "." + column.name;
        if ( alias == null ) {
            this.alias = this.fullyQualifiedName;
        } else {
            this.alias = alias;
        }
        this.aggregate = aggregate;
        this.explicit = explicit;
    }


    RequestColumn( LogicalColumn column, int tableScanIndex, int logicalIndex, String alias, AggFunction aggregate ) {
        this( column, tableScanIndex, logicalIndex, alias, aggregate, true );
    }


    public boolean isAggregateColumn() {
        return this.aggregate != null;
    }


    public boolean isAliased() {
        return this.alias != null;
    }


    public int getScanIndex() {
        return tableScanIndex;
    }

    // The alias of a column in a request perfectly identifies it when it comes to uniqueness!
    // This is due to the fact that the same CatalogColumn may be present multiple times iff it is aliased to different names.
    // This usually happens if aggregate functions are used.


    @Override
    public boolean equals( Object o ) {
        if ( this == o ) {
            return true;
        }
        if ( o == null || getClass() != o.getClass() ) {
            return false;
        }
        RequestColumn that = (RequestColumn) o;
        return alias.equals( that.alias );
    }


    @Override
    public int hashCode() {
        return Objects.hash( alias );
    }

}
