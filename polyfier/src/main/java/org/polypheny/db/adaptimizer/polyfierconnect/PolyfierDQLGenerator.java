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

package org.polypheny.db.adaptimizer.polyfierconnect;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Triple;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.adaptimizer.polyfierconnect.pseudo.Constructor;
import org.polypheny.db.adaptimizer.polyfierconnect.pseudo.struct.Alias;
import org.polypheny.db.adaptimizer.polyfierconnect.pseudo.struct.RandomHub;
import org.polypheny.db.adaptimizer.polyfierconnect.pseudo.struct.RandomNode;
import org.polypheny.db.adaptimizer.rndschema.DefaultTestEnvironment;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptSchema;
import org.polypheny.db.plan.Context;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Random;

@Slf4j
@AllArgsConstructor
public class PolyfierDQLGenerator {
    private final long seed;
    private final int nodes;
    private final List<CatalogTable> tables;
    private int count;
    private int batch;
    private final TransactionManager transactionManager;

    // private final List<DataStore> dataStores;

    public void generate() {

        RandomHub randomHub = new RandomHub( seed );
        RandomNode randomNode = randomHub.branchRoot();

        Transaction transaction;
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


        for ( int i = 0; i < batch; i++, count++ ) {
            randomNode = randomNode.branch("PolyfierDQLGenerator");

            Optional<Triple<Statement, AlgNode, Long>> tree = new Constructor( transaction, nodes ).construct( randomNode, tables );

            if ( tree.isPresent() ) {
                PolyfierResult polyfierResult = new PolyfierQueryExecutor( tree.get() ).execute().getResult();
                log.debug(String.valueOf(polyfierResult.success));
                log.debug( polyfierResult.logical );
                log.debug( polyfierResult.physical );
            } else {
                i--;
            }

        }

    }

    public static String testPolyfierDQLGeneration() {




        return "";
    }


}
