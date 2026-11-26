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

package org.polypheny.db.prisminterface.statements;

import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.prism.BatchGeneratedKeys;
import org.polypheny.prism.StatementBatchResponse;
import org.polypheny.prism.StatementResult;

public final class BatchProgress {

    private final int batchId;
    private final List<Member> completed = new ArrayList<>();


    public BatchProgress( int batchId ) {
        this.batchId = batchId;
    }


    void add( PIStatement statement, StatementResult result ) {
        completed.add( new Member( statement.getTransaction(), result ) );
    }


    StatementBatchResponse completeResponse() {
        return toResponse( completed );
    }


    public StatementBatchResponse partialResponse() {
        // A later failure can undo every member of the current transaction.
        return toResponse( completed.stream().takeWhile( member -> !member.transaction().isRolledBack() ).toList() );
    }


    private StatementBatchResponse toResponse( List<Member> members ) {
        StatementBatchResponse.Builder response = StatementBatchResponse.newBuilder().setBatchId( batchId );
        for ( int i = 0; i < members.size(); i++ ) {
            StatementResult result = members.get( i ).result();
            response.addScalars( result.getScalar() );
            if ( result.hasGeneratedKeys() ) {
                response.addGeneratedKeys( BatchGeneratedKeys.newBuilder().setStatementIndex( i ).setKeys( result.getGeneratedKeys() ) );
            }
        }
        return response.build();
    }


    private record Member( Transaction transaction, StatementResult result ) {
    }

}
