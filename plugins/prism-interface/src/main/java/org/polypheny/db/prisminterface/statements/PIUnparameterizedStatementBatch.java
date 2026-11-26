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
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.prisminterface.PIClient;
import org.polypheny.db.prisminterface.PIServiceException;
import org.polypheny.db.prisminterface.statementProcessing.StatementProcessor;
import org.polypheny.db.prisminterface.utils.PropertyUtils;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;

@Getter
public class PIUnparameterizedStatementBatch {

    private final List<PIUnparameterizedStatement> statements;
    private final PIClient prismInterfaceClient;
    private final int batchId;
    private final boolean includesGraphStatement;


    public PIUnparameterizedStatementBatch( int batchId, PIClient prismInterfaceClient, List<PIUnparameterizedStatement> statements ) {
        this.statements = statements;
        this.prismInterfaceClient = prismInterfaceClient;
        this.batchId = batchId;
        this.includesGraphStatement = statements.stream().anyMatch( statement -> statement.getLanguage().dataModel() == DataModel.GRAPH );
    }


    public List<Node> validate() {
        if ( !includesGraphStatement ) {
            return Collections.nCopies( statements.size(), null );
        }
        List<Node> parsed = statements.stream().map( StatementProcessor::parseUpdateBatchMember ).toList();
        if ( parsed.size() > 1 && parsed.stream().anyMatch( Node::isDdl ) ) {
            // Preparing DDL commits the transaction immediately.
            throw new PIServiceException( "DDL must execute separately from other statements in a Cypher batch", "0A000" );
        }
        return parsed;
    }


    public List<Long> executeBatch( List<Node> parsedStatements ) {
        return includesGraphStatement
                ? prismInterfaceClient.executeAtomicBatch( () -> executeMembers( parsedStatements ) )
                : executeMembers( parsedStatements );
    }


    private List<Long> executeMembers( List<Node> parsedStatements ) {
        int fetchSize = PropertyUtils.DEFAULT_FETCH_SIZE;
        List<List<ParsedQueryContext>> preParsedQueries = statements.stream()
                .map( StatementProcessor::parseWithOpenAutocommitResult )
                .toList();
        List<Long> updateCounts = new ArrayList<>( statements.size() );
        for ( int i = 0; i < statements.size(); i++ ) {
            PIUnparameterizedStatement statement = statements.get( i );
            updateCounts.add( statement.executeBatchMember( parsedStatements.get( i ), fetchSize, preParsedQueries.get( i ) ).getScalar() );
            // The batch response exposes counts only and cannot be used to fetch a member cursor.
            statement.closeResultsAndCommitIfAuto();
        }
        return updateCounts;
    }

}
