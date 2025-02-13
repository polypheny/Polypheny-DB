/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.transaction.locking;

import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.transaction.Transaction;

public class LpgGarbageCollector implements GarbageCollector {

    private static final String LPG_CLEANUP_LANGUAGE = "cypher";

    private static final String GRAPH_CLEANUP_STATEMENT = """
            MATCH (t)
            OPTIONAL MATCH (m)
            WHERE t._vid < %d AND (
                EXISTS {
                    MATCH (m)
                    WHERE ABS(m._eid) = ABS(t._eid)
                    WITH m, MAX(m._vid) AS max_vid
                    RETURN max_vid
                } >= %d
                OR t._vid < (
                    MATCH (m)
                    WHERE ABS(m._eid) = ABS(t._eid)
                    RETURN MAX(m._vid)
                )
                OR t._eid < 0
            )
            DETACH DELETE t
            """;


    @Override
    public void collect( LogicalEntity entity, long lowestActiveVersion, Transaction transaction ) {
        // ToDo TH: implement
        //throw new NotImplementedException();
    }

}
