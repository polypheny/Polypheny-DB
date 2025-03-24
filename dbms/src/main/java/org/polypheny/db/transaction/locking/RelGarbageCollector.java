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

import java.util.HashSet;
import java.util.Set;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.mvcc.MvccUtils;

public class RelGarbageCollector implements GarbageCollector {

    private static final String REL_CLEANUP_LANGUAGE = "sql";

    private static final String REL_RELEASED_EID_QUERY = """
            SELECT ABS(t._eid) AS released_eid
            FROM %s AS t
            WHERE t._eid < 0
              AND t._vid < %d;
            """;

    private static final String REL_CLEANUP_QUERY = """
            DELETE FROM %s
            WHERE EXISTS (
                SELECT 1
                FROM (
                    SELECT
                        ABS(_eid) AS eid,
                        MAX(_vid) AS max_vid
                    FROM
                        %s
                    GROUP BY
                        ABS(_eid)
                ) AS _MaxVid
                JOIN %s AS t
                ON ABS(t._eid) = _MaxVid.eid
                WHERE
                    t._vid < %d
                    AND (
                        _MaxVid.max_vid >= %d
                        OR t._vid < _MaxVid.max_vid
                        OR t._eid < 0
                    )
                AND %s._eid = t._eid
                AND %s._vid = t._vid
            );
            """;


    @Override
    public void collect( LogicalEntity entity, long lowestActiveVersion, Transaction transaction ) {
        String releasedEidQuery = String.format(
                REL_RELEASED_EID_QUERY,
                entity.getName(),
                lowestActiveVersion
        );

        Set<Long> releasedIds = new HashSet<>(); //ToDo TH: this will become a problem with table size
        Statement statement1 = transaction.createStatement();
        ResultIterator iterator = MvccUtils.executeStatement( QueryLanguage.from( REL_CLEANUP_LANGUAGE ), releasedEidQuery, entity.getNamespaceId(), statement1, transaction ).getIterator();
        iterator.getIterator().forEachRemaining( r -> releasedIds.add( r[0].asLong().longValue() ) );
        iterator.close();
        statement1.close();

        String query = String.format(
                REL_CLEANUP_QUERY,
                entity.getName(),
                entity.getName(),
                entity.getName(),
                lowestActiveVersion,
                lowestActiveVersion,
                entity.getName(),
                entity.getName()
        );
        Statement statement2 = transaction.createStatement();
        MvccUtils.executeStatement( QueryLanguage.from( REL_CLEANUP_LANGUAGE ), query, entity.getNamespaceId(), statement2, transaction );
        statement2.close();

        entity.getEntryIdentifiers().releaseEntryIdentifiers( releasedIds );
    }


}
