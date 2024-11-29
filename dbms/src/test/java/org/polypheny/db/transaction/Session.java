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

package org.polypheny.db.transaction;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import org.polypheny.db.TestHelper;

public class Session {

    @Getter
    private Transaction transaction;
    private final TestHelper testHelper;
    ExecutorService executorService;


    public Session( TestHelper testHelper ) {
        this.testHelper = testHelper;
        this.executorService = Executors.newSingleThreadExecutor();
    }


    boolean awaitCompletion() throws InterruptedException {
        executorService.shutdown();
        return executorService.awaitTermination( 60, TimeUnit.SECONDS );
    }


    private void addOperation( Runnable operation ) {
        if (executorService.isShutdown() || executorService.isTerminated()) {
            executorService = Executors.newSingleThreadExecutor();
        }
        executorService.submit( operation );
    }


    void startTransaction() {
        addOperation( () -> transaction = testHelper.getTransaction() );
    }


    void executeStatement( String query, String languageName ) {
        addOperation( () -> ConcurrencyTestUtils.executeStatement( query, languageName, transaction, testHelper ).forEach( r -> r.getIterator().close() ) );
    }


    void commitTransaction() {
        addOperation( () -> transaction.commit() );
    }

}

