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

package org.polypheny.db.adaptimizer.rndqueries;


import java.util.function.Supplier;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Triple;
import org.polypheny.db.adaptimizer.AdaptiveOptimizerImpl;
import org.polypheny.db.adaptimizer.rnddata.RndDataException;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionManager;

public abstract class AbstractQuerySupplier implements Supplier<Triple<Statement, AlgNode, Long>> {
    @Getter(AccessLevel.PUBLIC)
    protected final AbstractQueryGenerator treeGenerator;
    @Getter(AccessLevel.PRIVATE)
    protected final TransactionManager transactionManager;
    @Getter(AccessLevel.PRIVATE)
    protected final Catalog catalog;

    Transaction transaction;

    protected AbstractQuerySupplier( AbstractQueryGenerator treeGenerator ) {
        this.treeGenerator = treeGenerator;
        this.transactionManager = AdaptiveOptimizerImpl.getTransactionManager();
        this.catalog = AdaptiveOptimizerImpl.getCatalog();
        transaction = getTransaction();
    }

    protected Statement nextStatement() {
        // Currently, one statement per transaction...
        return this.transaction.createStatement();
        // return getTransaction( catalog ).createStatement();
    }

    private Transaction getTransaction() {
        Transaction transaction;
        try {
            transaction = transactionManager.startTransaction(
                    Catalog.defaultUserId,
                    Catalog.defaultDatabaseId,
                    false,
                    "AdaptExecutionUtil"
            );
        } catch ( UnknownDatabaseException | GenericCatalogException | UnknownUserException | UnknownSchemaException e ) {
            e.printStackTrace();
            throw new RndDataException( "Could not start transaction", e );
        }
        return transaction;
    }


}
