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

package org.polypheny.db.adaptimizer.randomtrees;

import java.util.Stack;
import java.util.function.Supplier;
import java.util.stream.LongStream;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Triple;
import org.polypheny.db.adaptimizer.randomdata.except.TestDataGenerationException;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.Pair;

/**
 * Wrapper for the RelRandomTreeGenerator.
 */
public class TreeGenerator implements Supplier<Triple<Statement, AlgNode, Long>> {

    private final Catalog catalog;
    private final TransactionManager transactionManager;
    private final RelRandomTreeGenerator relTreeGenerator;

    @Getter
    private Transaction currentTransaction;

    @Getter
    private int successCounter;

    @Getter
    private int failureCounter;

    @Getter
    private int treeNodeCounter;

    @Getter
    private String stringRep;

    private final Stack<Long> treeGenTimes;

    public TreeGenerator( Catalog catalog, TransactionManager transactionManager, RelRandomTreeTemplate relRandomTreeTemplate ) {
        this.catalog = catalog;
        this.transactionManager = transactionManager;
        this.relTreeGenerator = new RelRandomTreeGenerator( relRandomTreeTemplate );
        this.currentTransaction = this.getTransaction( catalog );

        this.treeGenTimes = new Stack<>();
        this.successCounter = 0;
        this.failureCounter = 0;
        this.treeNodeCounter = 0;
    }

    @Override
    public Triple<Statement, AlgNode, Long> get() {
        Statement statement = this.currentTransaction.createStatement();

        --this.failureCounter;
        Pair<AlgNode, Long> tree = new Pair<>( null, null );
        while ( tree.left == null ) {
            ++this.failureCounter;
            tree = this.relTreeGenerator.generate( statement );
            this.treeNodeCounter += this.relTreeGenerator.getNodeCounter();
            this.treeGenTimes.push( this.relTreeGenerator.getTreeGenTime() );
        }

        ++this.successCounter;
        this.stringRep = this.relTreeGenerator.getStringRep();

        return Triple.of( statement, tree.left, tree.right );
    }


    /**
     * Sets the seed for the next tree generated.
     */
    public void setSeed( long seed ) {
        this.relTreeGenerator.setSeed( seed );
    }


    private void nextTransaction() {
        this.currentTransaction = getTransaction( this.catalog );
    }


    /**
     * Commits the current transaction.
     * @param next  If true, create a new transaction.
     */
    public void commitTransaction( boolean next ) {
        try {
            this.currentTransaction.commit();
        } catch ( TransactionException e ) {
            e.printStackTrace();
            this.rollbackTransaction( next );
        } finally {
            if ( next ) {
                this.nextTransaction();
            }
        }
    }

    /**
     * Rolls back the current transaction.
     * @param next  If true, create a new transaction.
     */
    public void rollbackTransaction( boolean next ) {
        try {
            this.currentTransaction.rollback();
        } catch ( TransactionException e ) {
            e.printStackTrace();
        } finally {
            if ( next ) {
                this.nextTransaction();
            }
        }
    }


    private Transaction getTransaction( Catalog catalog ) {
        Transaction transaction;
        try {
            transaction = this.transactionManager.startTransaction(
                    catalog.getUser( Catalog.defaultUserId ).name,
                    catalog.getDatabase( Catalog.defaultDatabaseId ).name,
                    true,
                    "TreeGenerator - getTransaction"
            );
        } catch ( UnknownDatabaseException | GenericCatalogException | UnknownUserException | UnknownSchemaException e ) {
            e.printStackTrace();
            throw new TestDataGenerationException( "Could not start transaction", e );
        }
        return transaction;
    }


    public double getTime() {
        return this.treeGenTimes.stream().mapToLong( Long::longValue ).sum();
    }

    public double getAvgTime() {
        return ( float ) this.treeGenTimes.stream().mapToLong( Long::longValue ).sum() / this.treeGenTimes.size();
    }

    public double getFailureRate() {
        return ( ( float ) failureCounter / ( successCounter + failureCounter ) ) * 100;
    }


}
