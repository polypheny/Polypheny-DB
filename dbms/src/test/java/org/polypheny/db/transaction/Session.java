package org.polypheny.db.transaction;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import org.polypheny.db.TestHelper;
import org.polypheny.db.processing.ImplementationContext.ExecutedContext;

public class Session {

    private final TestHelper testHelper;
    @Getter
    private Transaction transaction;
    private ExecutorService executorService;


    public Session( TestHelper testHelper ) {
        this.testHelper = testHelper;
        this.executorService = Executors.newSingleThreadExecutor();
    }


    boolean awaitCompletion() throws InterruptedException {
        executorService.shutdown();
        return executorService.awaitTermination( 60, TimeUnit.SECONDS );
    }


    Future<List<ExecutedContext>> executeStatement( String query, String languageName ) {
        if ( executorService.isShutdown() || executorService.isTerminated() ) {
            executorService = Executors.newSingleThreadExecutor();
        }
        return executorService.submit( () -> ConcurrencyTestUtils.executeStatement( query, languageName, transaction, testHelper ) );
    }

    void executeStatementIgnoreResult( String query, String languageName ) {
        if ( executorService.isShutdown() || executorService.isTerminated() ) {
            executorService = Executors.newSingleThreadExecutor();
        }
        executorService.submit( () -> ConcurrencyTestUtils.executeStatement( query, languageName, transaction, testHelper ).forEach( r -> r.getIterator().getAllRowsAndClose() ) );
    }


    void startTransaction() {
        this.transaction = testHelper.getTransaction();
    }


    void commitTransaction() {
        executorService.submit(() -> transaction.commit());
    }


    void rollbackTransaction() {
        executorService.submit(() -> transaction.rollback( "Requested by TestCase." ));
    }

}
