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

package org.polypheny.db.adaptimizer.randomtrees;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.polypheny.db.util.Pair;

@AllArgsConstructor
@Getter
public class AdaptiveTableRecord implements TableRecord {

    private final String tableName;
    private final List<String> columns;

    public static AdaptiveTableRecord from( AdaptiveTableRecord a ) {
        List<String> columns = List.copyOf( a.getColumns() );
        return new AdaptiveTableRecord(
            a.getTableName(),
            columns
        );
    }

    public static AdaptiveTableRecord join( AdaptiveTableRecord a, AdaptiveTableRecord b, String joinA, String joinB ) {
        List<String> columns = new LinkedList<>();

        List<String> listA = new ArrayList<>( a.getColumns() );
        List<String> listB = new ArrayList<>( b.getColumns() );

        listB.remove( joinB );

        columns.addAll( listA );
        columns.addAll( listB );

        return new AdaptiveTableRecord(
                null,
                columns
        );
    }

    public static AdaptiveTableRecord project( AdaptiveTableRecord a, List<String> columns ) {
        return new AdaptiveTableRecord(
                null,
                a.getColumns().stream().filter( str -> ! columns.contains( str ) ).collect( Collectors.toList())
        );
    }

}
