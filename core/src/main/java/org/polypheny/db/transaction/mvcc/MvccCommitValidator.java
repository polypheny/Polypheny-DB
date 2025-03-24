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

package org.polypheny.db.transaction.mvcc;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.locking.SequenceNumberGenerator;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;

public class MvccCommitValidator {
    Transaction transaction;
    public MvccCommitValidator(Transaction transaction) {
        this.transaction = transaction;
    }

    public boolean validateWriteSet(Set<Entity> writtenEntities) {
        long maxVersion = 0;

        for ( Entity writtenEntity : writtenEntities ) {
            if (transaction.getSnapshot().getLogicalEntity( writtenEntity.getId() ).isEmpty()) {
                continue;
            }
            maxVersion = Math.max( maxVersion, switch ( writtenEntity.getDataModel() ) {
                case RELATIONAL -> validateRelWrites( writtenEntity);
                case DOCUMENT -> validateDocWrites( writtenEntity);
                case GRAPH -> validateGraphWrites( writtenEntity);
            } );
        }

        return maxVersion <= transaction.getSequenceNumber();
    }

    private long validateRelWrites(Entity writtenEntity) {
        String queryTemplate = """
                SELECT CAST(MAX(a._vid) AS DECIMAL) AS max_vid
                FROM %s AS a
                JOIN %s AS b
                  ON ABS(a._eid) = ABS(b._eid)
                WHERE b._vid = %d;
                """;

        String query = String.format( queryTemplate, writtenEntity.getName(), writtenEntity.getName(), -transaction.getSequenceNumber() );
        List<List<PolyValue>> res;
        try ( ResultIterator iterator = MvccUtils.executeStatement( QueryLanguage.from( "sql" ), query, writtenEntity.getNamespaceId(), transaction ).getIterator() ) {
            res = iterator.getNextBatch();
            return MvccUtils.collectLong( res.get( 0 ).get( 0 ) );
        }
    }


    private long validateDocWrites(Entity writtenEntity) {
        // ToDo TH: Optimization - Replace with more efficient mechanism once group and addToSet are supported

        // step 1: get list of all written document entry ids
        String writeSetTemplate = "db.%s.find( { \"_vid\": %d }, { \"_eid\": 1 } );";

        String writeSetQuery = String.format( writeSetTemplate, writtenEntity.getName(), -transaction.getSequenceNumber() );
        HashSet<Long> entryIds = new HashSet<>();
        try ( ResultIterator iterator = MvccUtils.executeStatement( QueryLanguage.from( "mql" ), writeSetQuery, writtenEntity.getNamespaceId(), transaction ).getIterator() ) {
            iterator.getIterator().forEachRemaining(
                    r -> entryIds.add(r[0].asDocument().get( IdentifierUtils.getIdentifierKeyAsPolyString() ).asBigDecimal().longValue())
            );
        }

        // step 2: get max of version ids present for each of the written entry ids
        String getMaxTemplate = "db.%s.aggregate(["
                + "    { \"$match\": { \"_eid\": { \"$in\": [%s] } } },"
                + "    { \"$group\": { \"_id\": null, \"max_vid\": { \"$max\": \"$_vid\" } } }"
                + "]);";

        String entryIdString = entryIds.stream().map( String::valueOf ).collect( Collectors.joining( ", " ) );
        String getMaxQuery = String.format( getMaxTemplate, writtenEntity.getName(), entryIdString );
        try ( ResultIterator iterator = MvccUtils.executeStatement( QueryLanguage.from( "mql" ), getMaxQuery, writtenEntity.getNamespaceId(), transaction ).getIterator() ) {
            PolyDocument result = iterator.getNextBatch().get( 0 ).get( 0 ).asDocument();
            return MvccUtils.collectLong( result.get( PolyString.of( "max_vid" ) ) );
        }
    }


    public long validateGraphWrites(Entity writtenEntity) {
        throw new NotImplementedException();
        //ToDo TH: implement this
    }


    public long updateWrittenVersionIds(Set<Entity> writtenEntities) {
        long commitSequenceNumber = SequenceNumberGenerator.getInstance().getNextNumber();

        for ( Entity writtenEntity : writtenEntities ) {
            if (transaction.getSnapshot().getLogicalEntity( writtenEntity.getId() ).isEmpty()) {
                continue;
            }
            switch ( writtenEntity.getDataModel() ) {
                case RELATIONAL -> updateWrittenRelVersionIds(commitSequenceNumber, writtenEntity);
                case DOCUMENT -> updateWrittenDocVersionIds(commitSequenceNumber, writtenEntity);
                case GRAPH -> updateWrittenGraphVersionIds(commitSequenceNumber, writtenEntity);
            }
        }

        SequenceNumberGenerator.getInstance().releaseNumber( transaction.getSequenceNumber() );
        return commitSequenceNumber;
    }


    private void updateWrittenRelVersionIds( long commitSequenceNumber, Entity writtenEntity) {
        String queryTemplate = """
                UPDATE %s
                SET _vid = %d
                WHERE _vid = %d
                """;

        String query = String.format( queryTemplate, writtenEntity.getName(), commitSequenceNumber, -transaction.getSequenceNumber() );
        MvccUtils.executeStatement( QueryLanguage.from( "sql" ), query, writtenEntity.getNamespaceId(), transaction );
    }


    private void updateWrittenGraphVersionIds( long commitSequenceNumber, Entity writtenEntity) {
        throw new NotImplementedException();
        //ToDo TH: implement this
    }


    private void updateWrittenDocVersionIds( long commitSequenceNumber, Entity writtenEntity ) {
        String updateTemplate = """
                        db.%s.updateMany(
                        { _vid: %d },
                        { $set: { _vid: %d } }
                        );
                """;

        String updateQuery = String.format( updateTemplate,
                writtenEntity.getName(),
                -transaction.getSequenceNumber(),
                commitSequenceNumber );
        MvccUtils.executeStatement( QueryLanguage.from( "mql" ), updateQuery, writtenEntity.getNamespaceId(), transaction );
    }
}
