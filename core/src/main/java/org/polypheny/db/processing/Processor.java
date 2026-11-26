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

package org.polypheny.db.processing;


import java.util.List;
import java.util.Map;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.constant.ResultKind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.routing.ExecutionTimeMonitor;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.locking.Lockable;
import org.polypheny.db.transaction.locking.Lockable.LockType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.DeadlockException;
import org.polypheny.db.util.Pair;

@Slf4j
public abstract class Processor {

    public abstract List<? extends Node> parse( String query );


    public PreparedNamedQuery prepareNamedQuery( String query ) {
        throw new UnsupportedOperationException( "Named parameters are not supported by this query processor" );
    }


    public PreparedNamedQuery prepareNamedQuery( String query, LogicalNamespace namespace ) {
        return prepareNamedQuery( query );
    }


    /** A parsed parameter signature and a binder that creates a fresh AST for each execution. */
    public record PreparedNamedQuery(List<String> parameterNames, Function<Map<String, PolyValue>, Node> bind) {
    }


    public abstract Pair<Node, AlgDataType> validate( Transaction transaction, Node parsed, boolean addDefaultValues );

    public abstract AlgRoot translate( Statement statement, ParsedQueryContext context );


    public PolyImplementation prepareDdl( Statement statement, ExecutableStatement node, ParsedQueryContext context ) {
        try {
            Map<Lockable, LockType> requiredLockables = node.deriveLockables( statement.getPrepareContext(), context );
            Transaction transaction = statement.getTransaction();
            requiredLockables.forEach( transaction::acquireLockable );
            transaction.acquireCatalogWriteLock();
            return getImplementation( statement, node, context );
        } catch ( DeadlockException e ) {
            throw new DeadlockException( " Exception while acquiring global schema lock", e );
        }
    }


    PolyImplementation getImplementation( Statement statement, ExecutableStatement node, ParsedQueryContext context ) throws TransactionException {
        node.execute( statement.getPrepareContext(), statement, context );
        statement.getTransaction().commit();

        return new PolyImplementation(
                null,
                context.getLanguage().dataModel(),
                new ExecutionTimeMonitor(),
                null,
                Kind.CREATE_NAMESPACE, // technically correct, maybe change
                statement,
                null,
                ResultKind.COMMAND );
    }


    protected abstract void lock( Transaction transaction, ParsedQueryContext context ) throws DeadlockException;

    public abstract String getQuery( Node parsed, QueryParameters parameters );

    public abstract AlgDataType getParameterRowType( Node left );

    public abstract List<String> splitStatements( String statements );


    /** Whether a parsed statement is a mutation or DDL that returns no result rows. */
    public boolean isUpdateOnly( Node statement ) {
        return statement.isDdl() || Kind.DML.contains( statement.getKind() );
    }


    /** Whether each binding must observe changes made by earlier bindings in its batch. */
    public boolean requiresSequentialBatchExecution( Node statement, AlgDataType parameterRowType ) {
        return false;
    }

}
