/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.polyfier.core;

import com.google.common.annotations.Beta;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Triple;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.polyfier.core.construct.DqlConstructor;
import org.polypheny.db.polyfier.core.construct.model.ColumnStatistic;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;

import java.util.HashMap;
import java.util.List;
import java.util.Optional;

@Slf4j
@Builder
public class PolyfierDQLGenerator {
    private final List<CatalogTable> tables;
    private final long seed;
    private final int nodes;
    private final TransactionManager transactionManager;
    private final HashMap<String, ColumnStatistic> columnStatistics;

    private Transaction transaction;

    @Getter
    private PolyfierQueryExecutor polyfierQueryExecutor;

    public Optional<PolyfierResult> generate() {
        try {
            transaction = transactionManager.startTransaction(
                    Catalog.defaultUserId,
                    Catalog.defaultDatabaseId,
                    false,
                    "PolyfierDQLGenerator"
            );
        } catch ( Exception e ) {
            throw new RuntimeException( "Could not create transaction." );
        }

        if ( log.isDebugEnabled() ) {
            log.debug("Call for Query Generation...");
        }
        Optional<Triple<Statement, AlgNode, Long>> tree = new DqlConstructor( transaction, nodes, columnStatistics ).construct( seed, tables );

        if ( tree.isPresent() ) {
            if ( tree.get().getMiddle() == null ) {
                if ( log.isDebugEnabled() ) {
                    log.debug("Conversion failure occurred and no AlgNode was given.");
                    return Optional.empty();
                }
            }
            if ( log.isDebugEnabled() ) {
                log.debug("Executing...");
            }
            polyfierQueryExecutor = new PolyfierQueryExecutor( tree.get() );
            PolyfierResult polyfierResult = polyfierQueryExecutor.execute().getResult();
            handleTransaction();
            return Optional.of( polyfierResult );
        } else {
            if ( log.isDebugEnabled() ) {
                log.debug("Could not find a working tree." );
            }
            handleTransaction();
            return Optional.empty();
        }

    }

    @Beta
    public Optional<String> testGenerate() {
        try {
            transaction = transactionManager.startTransaction(
                    Catalog.defaultUserId,
                    Catalog.defaultDatabaseId,
                    false,
                    "PolyfierDQLGenerator"
            );
        } catch ( Exception e ) {
            throw new RuntimeException( "Could not create transaction." );
        }

        Optional<Triple<Statement, AlgNode, Long>> tree = new DqlConstructor( transaction, nodes, columnStatistics ).construct( seed, tables );

        if (tree.isEmpty() || tree.get().getMiddle() == null ) {
            return Optional.empty();
        }

        return Optional.of("");

    }

    private void handleTransaction() {
        if ( transaction.isActive() ) {
            try {
                transaction.commit();
            } catch (TransactionException e) {
                try {
                    transaction.rollback();
                } catch (TransactionException ex) {
                    throw new RuntimeException(ex);
                }
            }
        }
    }

}
