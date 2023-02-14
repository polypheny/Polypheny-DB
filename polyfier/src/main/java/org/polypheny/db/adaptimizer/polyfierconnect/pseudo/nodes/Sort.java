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

package org.polypheny.db.adaptimizer.polyfierconnect.pseudo.nodes;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.checkerframework.checker.units.qual.A;
import org.polypheny.db.adaptimizer.polyfierconnect.pseudo.struct.Alias;
import org.polypheny.db.adaptimizer.polyfierconnect.pseudo.struct.Column;
import org.polypheny.db.adaptimizer.polyfierconnect.pseudo.struct.Result;
import org.polypheny.db.http.model.SortDirection;
import org.polypheny.db.http.model.SortState;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.Pair;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter(AccessLevel.PROTECTED)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class Sort extends Unary {

    ArrayList<Pair<Alias, SortState>> sortTargets;

    public static Sort sort( SortDirection sortDirection, List<Column> sortColumns ) {
        Sort sort = new Sort();

        sort.setOperatorType( OperatorType.SORT );

        sort.setSortTargets( new ArrayList<>() );
        sortColumns.forEach(column -> {
            SortState sortState = new SortState();
            sortState.column = column.getName();
            sortState.sorting = true;
            sortState.direction = sortDirection;
            column.getAlias().ifPresentOrElse(
                    alias -> sort.getSortTargets().add( Pair.of( alias, sortState )),
                    () -> sort.getSortTargets().add( Pair.of( null, sortState ))
            );
        } );
        sort.setTarget( sortColumns.get( 0 ).getResult().getNode() );
        sort.setResult( Result.from( sort, sort.getTarget()) );
        return sort;
    }

}