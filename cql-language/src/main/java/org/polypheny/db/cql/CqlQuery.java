/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.cql;

import java.util.List;
import java.util.Map;
import org.polypheny.db.cql.BooleanGroup.ColumnOpsBooleanOperator;
import org.polypheny.db.cql.utils.Tree;
import org.polypheny.db.util.Pair;


/**
 * Packaging all the information in a CQL query together.
 */
public class CqlQuery {

    public final Tree<Combiner, TableIndex> queryRelation;
    public final Tree<BooleanGroup<ColumnOpsBooleanOperator>, Filter> filters;
    public final Map<String, TableIndex> tableIndexMapping;
    public final Map<String, ColumnIndex> columnIndexMapping;
    public final List<Pair<ColumnIndex, Map<String, Modifier>>> sortSpecifications;
    public final Projections projections;


    public CqlQuery(
            final Tree<Combiner, TableIndex> queryRelation,
            final Tree<BooleanGroup<ColumnOpsBooleanOperator>, Filter> filters,
            final Map<String, TableIndex> tableIndexMapping,
            final Map<String, ColumnIndex> columnIndexMapping,
            final List<Pair<ColumnIndex, Map<String, Modifier>>> sortSpecifications,
            final Projections projections ) {
        this.queryRelation = queryRelation;
        this.filters = filters;
        this.tableIndexMapping = tableIndexMapping;
        this.columnIndexMapping = columnIndexMapping;
        this.sortSpecifications = sortSpecifications;
        this.projections = projections;
    }


    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();

        if ( filters != null ) {
            stringBuilder.append( filters );
        }
        stringBuilder.append( "relation " ).append( queryRelation );
        if ( sortSpecifications != null && sortSpecifications.size() != 0 ) {
            stringBuilder.append( " " );
            for ( Pair<ColumnIndex, Map<String, Modifier>> sortSpecification : sortSpecifications ) {
                stringBuilder.append( sortSpecification.toString() );
            }
            stringBuilder.append( " " );
        }

        return stringBuilder.toString();
    }

}
