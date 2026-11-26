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

import java.util.List;
import java.util.function.Supplier;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.prisminterface.PIClient;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Statement.GeneratedKeySelection;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.prism.ColumnMeta;
import org.polypheny.prism.Frame;

@Getter
@Slf4j
public abstract class PIStatement {

    protected final int id;
    protected final PIClient client;
    protected final StopWatch executionStopWatch;
    protected final QueryLanguage language;
    @Setter
    private ResultCursor resultCursor;
    @Setter
    private GeneratedKeySelection generatedKeySelection;
    protected LogicalNamespace namespace;


    protected PIStatement(
            int id, @NotNull PIClient client,
            @NotNull QueryLanguage language,
            @NotNull LogicalNamespace namespace ) {
        this.id = id;
        this.client = client;
        this.language = language;
        this.executionStopWatch = new StopWatch();
        this.namespace = namespace;
    }


    protected <T> T executeInTransaction( boolean validatedOverlappingRead, Supplier<T> execution ) {
        closeResultsAndCommitIfAuto();
        Transaction transaction = validatedOverlappingRead && client.getStatementManager().hasOpenResult( false )
                ? client.startIsolatedReadTransaction()
                : client.getOrCreateNewTransaction();
        try {
            Statement previous = getStatement();
            if ( previous == null || previous.getTransaction() != transaction ) {
                if ( previous != null ) {
                    previous.close();
                }
                setStatement( transaction.createStatement() );
            } else {
                previous.getDataContext().resetParameterValues();
            }
            Statement current = getStatement();
            current.setInsertReturning( null );
            current.setGeneratedKeysProjection( null );
            current.setGeneratedKeySelection( generatedKeySelection );
            current.setInsertedRows( List.of() );
            return execution.get();
        } catch ( Throwable failure ) {
            try {
                client.rollbackAutocommitTransaction( transaction, "Prism statement failed" );
            } catch ( RuntimeException cleanupFailure ) {
                failure.addSuppressed( cleanupFailure );
            }
            throw failure;
        }
    }


    public void closeResults() {
        ResultCursor cursor = resultCursor;
        resultCursor = null;
        if ( cursor != null ) {
            cursor.iterator().close();
        }
    }


    public void closeResultsAndCommitIfAuto() {
        ResultCursor cursor = resultCursor;
        closeResults();
        if ( cursor != null ) {
            client.getStatementManager().commitIfAutocommitAndNoOpenResults( cursor.transaction() );
        }
    }


    public record ResultCursor( ResultIterator iterator, Transaction transaction, Frame.ResultCase format, List<ColumnMeta> columns ) {

        public ResultCursor {
            columns = List.copyOf( columns );
        }

    }


    public abstract void close();

    public abstract PolyImplementation getImplementation();

    public abstract void setImplementation( PolyImplementation implementation );

    public abstract void setStatement( Statement statement );

    public abstract Statement getStatement();

    public abstract String getQuery();

    public abstract Transaction getTransaction();

}
