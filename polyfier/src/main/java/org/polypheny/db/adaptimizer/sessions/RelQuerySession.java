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

package org.polypheny.db.adaptimizer.sessions;


import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.adaptimizer.rndqueries.AbstractQuerySupplier;
import org.polypheny.db.adaptimizer.rndqueries.QuerySupplier;
import org.polypheny.db.algebra.constant.ExplainFormat;
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationQueryPlan;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;

@Slf4j
public class RelQuerySession extends QuerySession {
    private static final int SHOW_LIM = 15;
    private static int SHOW = 0;

    public RelQuerySession( AbstractQuerySupplier querySupplier ) {
        super( querySupplier );
    }

    @Override
    public void run() {
        if ( log.isDebugEnabled() ) {
            log.debug(
                    "[ {} /w sid: {} /w init-seed: {} /w {}/{}q ]",
                    getSessionData().getSessionThread().getName(),
                    getSessionData().getSessionId(),
                    getSessionData().getInitialSeed(),
                    getSessionData().getQueriesExecuted(),
                    getSessionData().getOrderedQueries()
            );
        }

        final StopWatch stopWatch = new StopWatch();
        while ( getSessionData().getQueriesExecuted() < getSessionData().getOrderedQueries() ) {
            Stream.generate( getQuerySupplier() ).limit( 50 ).forEach( execTriple -> {
                try {
                    Thread.sleep( 5 );
                    stopWatch.start();
                    getQueryService().executeTree(
                            execTriple.getLeft(),
                            execTriple.getMiddle()
                    );
                    stopWatch.stop();
                    concludeTransaction(
                            ExecutionResult.SUCCESS,
                            null,
                            execTriple.getLeft().getTransaction(),
                            execTriple.getRight(),
                            stopWatch.getTime( TimeUnit.MILLISECONDS )
                    );
                    log.info(AlgOptUtil.dumpPlan("", execTriple.getMiddle(), ExplainFormat.JSON, ExplainLevel.NO_ATTRIBUTES));
                } catch (  Error error ) {
                    stopWatch.stop();
                    concludeTransaction(
                            ExecutionResult.ERROR,
                            error,
                            execTriple.getLeft().getTransaction(),
                            execTriple.getRight(),
                            stopWatch.getTime( TimeUnit.MILLISECONDS )
                    );
                } catch ( Exception exception ) {
                    stopWatch.stop();
                    concludeTransaction(
                            ExecutionResult.EXCEPTION,
                            exception,
                            execTriple.getLeft().getTransaction(),
                            execTriple.getRight(),
                            stopWatch.getTime( TimeUnit.MILLISECONDS )
                    );
                } finally {
                    stopWatch.reset();
                }
            } );
        }

        getSessionData().earlyFaults = ((QuerySupplier)getQuerySupplier()).earlyFaults;
        getSessionData().finish();
    }

    private void concludeTransaction( ExecutionResult result, Throwable ex, Transaction transaction, long seed, long executionTime ) {

        String message = "";
        try {
            transaction.commit();
            if ( log.isDebugEnabled() ) {
                message = "[ " + result.name() + " - Transact. " + transaction.getId() + " + - Closed ( Committed ) ]";
            }
        } catch ( TransactionException commitException ) {
            if ( result == ExecutionResult.SUCCESS ) {
                // Seems like the transaction did not work after all?
                result = ExecutionResult.EXCEPTION;
                ex = commitException;
            }
            try {
                transaction.rollback();
                if ( log.isDebugEnabled() ) {
                    message = "[ " + result.name() + " - Transact. " + transaction.getId() + " + - Closed ( Rollback ) ]";
                }
            } catch ( TransactionException rollbackException ) {
                log.error( "Could not rollback", rollbackException );
                if ( log.isDebugEnabled() ) {
                    message = "[ " + result.name() + " - Transact. " + transaction.getId() + " + - ERROR ]";
                }
            }
        }

        if ( log.isDebugEnabled() ) {
            // log.debug( message );
        }

        getSessionData().addQueryExecutionTime( result, seed, executionTime, ex );
    }


}
