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
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.routing.ExecutionTimeMonitor;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.util.DeadlockException;
import org.polypheny.db.util.Pair;

@Slf4j
public abstract class Processor {

    public abstract List<? extends Node> parse( String query );

    public abstract Pair<Node, AlgDataType> validate( Transaction transaction, Node parsed, boolean addDefaultValues );

    public abstract AlgRoot translate( Statement statement, ParsedQueryContext context );


    public PolyImplementation prepareDdl( Statement statement, ExecutableStatement node, ParsedQueryContext context ) {
        try {
            // Acquire global schema lock
            lock( statement );
            // Execute statement
            return getImplementation( statement, node, context );
        } catch ( DeadlockException e ) {
            throw new GenericRuntimeException( "Exception while acquiring global schema lock", e );
        } catch ( TransactionException e ) {
            throw new GenericRuntimeException( e );
        } finally {
            // Release lock
            unlock( statement );
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
                null );
    }


    public abstract void unlock( Statement statement );

    protected abstract void lock( Statement statement ) throws DeadlockException;

    public abstract String getQuery( Node parsed, QueryParameters parameters );

    public abstract AlgDataType getParameterRowType( Node left );

    public abstract List<String> splitStatements( String statements );
}
