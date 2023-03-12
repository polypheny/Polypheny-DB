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

package org.polypheny.db.polyfier.core.construct.nodes;

import lombok.*;
import org.polypheny.db.polyfier.core.construct.model.Column;
import org.polypheny.db.polyfier.core.construct.model.Result;
import org.polypheny.db.util.Pair;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter(AccessLevel.PROTECTED)
public class Sort extends Unary {
    private static int idx = 0;

    ArrayList<Pair<String, SortColumn>> sortTargets;

    protected Sort() {
        super( idx++ );
    }

    @Getter
    @AllArgsConstructor
    public static class SortColumn {
        private final int direction;
        private final String column;
        private final boolean sorting;
    }

    public static Sort sort( int direction, List<Column> sortColumns ) {
        // direction in {-1 (descending), 1 (ascending)}
        Sort sort = new Sort();

        sort.setOperatorType( OperatorType.SORT );

        sort.setSortTargets( new ArrayList<>() );
        sortColumns.forEach(column -> {
            if ( ! column.getAliases().isEmpty() ) {
                sort.getSortTargets().add( Pair.of( column.recentAlias(), new SortColumn( direction, column.getName(), true ) ) );
            } else {
                sort.getSortTargets().add( Pair.of( null, new SortColumn( direction, column.getName(), true ) ) );
            }
        } );
        sort.setTarget( sortColumns.get( 0 ).getResult().getNode() );
        sort.setResult( Result.from( sort ) );
        return sort;
    }

}