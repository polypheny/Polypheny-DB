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

package org.polypheny.db.adaptimizer.rndqueries;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Triple;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.Pair;

@Slf4j
public class QuerySupplier extends AbstractQuerySupplier {

    public int earlyFaults = 0;

    public QuerySupplier( AbstractQueryGenerator treeGenerator ) {
        super( treeGenerator );
    }

    @Override
    public Triple<Statement, AlgNode, Long> get() {
        Statement statement = nextStatement();

        if ( log.isDebugEnabled() ) {
            // log.debug( "[ Transact. {} - Opened ]", statement.getTransaction().getId() );
        }

        Pair<AlgNode, Long> result = Pair.of( null, null );

        int earlyNull = -1;
        while ( result.left == null ) {
            earlyNull++;
            result = getTreeGenerator().generate( statement );
        }

        earlyFaults += earlyNull;
        if ( earlyFaults % 1000 == 0 ) {
            // Check Performance when randomly changing seeds based on early faults
            getTreeGenerator().setSeed( getTreeGenerator().getTemplate().nextLong() );
        }
        return Triple.of( statement, result.left, result.right );
    }

}
