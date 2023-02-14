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

package org.polypheny.db.adaptimizer.rnddata;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class ForeignKeyPaths extends HashMap<Long, ForeignKeyPath> {

    public ForeignKeyPaths( final Map<Long, Long> seed, final Map<Long, ColumnOption> options, final Map<Long, Integer> cSize, Collection<ColumnOption> columnOptions ) {
        columnOptions.stream().filter( columnOption -> columnOption.getReferencedColumn() != null ).map( columnOption -> new ForeignKeyPath(
                seed, options, cSize, columnOption
        ) ).forEach( foreignKeyPath -> this.put( foreignKeyPath.getStartColumn(), foreignKeyPath ) );
    }

    public ForeignKeyPath get( Long column ) {
        if ( super.containsKey( column ) ) {
            return super.get( column );
        }
        return null;
    }

    public void reset() {
        this.values().forEach( ForeignKeyPath::reset );
    }

}
