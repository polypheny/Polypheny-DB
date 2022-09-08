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

package org.polypheny.db.processing;


import java.util.List;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.ExplainFormat;
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.NoTablePrimaryKeyException;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationQueryPlan;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.routing.ExecutionTimeMonitor;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.util.DeadlockException;
import org.polypheny.db.util.Pair;


public abstract class Processor {

    public abstract List<? extends Node> parse( String query );

    public abstract Pair<Node, AlgDataType> validate( Transaction transaction, Node parsed, boolean addDefaultValues );

    public abstract AlgRoot translate( Statement statement, Node query, QueryParameters parameters );


    public PolyImplementation prepareDdl( Statement statement, Node parsed, QueryParameters parameters ) {
        if ( parsed instanceof ExecutableStatement ) {
            try {
                // Acquire global schema lock
                lock( statement );
                // Execute statement
                return getResult( statement, parsed, parameters );
            } catch ( DeadlockException e ) {
                throw new RuntimeException( "Exception while acquiring global schema lock", e );
            } catch ( TransactionException | NoTablePrimaryKeyException e ) {
                throw new RuntimeException( e );
            } finally {
                // Release lock
                unlock( statement );
            }
        } else {
            throw new RuntimeException( "All DDL queries should be of a type that inherits ExecutableStatement. But this one is of type " + parsed.getClass() );
        }
    }


    PolyImplementation getResult( Statement statement, Node parsed, QueryParameters parameters ) throws TransactionException, NoTablePrimaryKeyException {
        ((ExecutableStatement) parsed).execute( statement.getPrepareContext(), statement, parameters );
        statement.getTransaction().commit();
        Catalog.getInstance().commit();
        return new PolyImplementation(
                null,
                parameters.getNamespaceType(),
                new ExecutionTimeMonitor(),
                null,
                Kind.CREATE_SCHEMA, // technically correct, maybe change
                statement,
                null );
    }


    public abstract void unlock( Statement statement );

    protected abstract void lock( Statement statement ) throws DeadlockException;

    public abstract String getQuery( Node parsed, QueryParameters parameters );

    public abstract AlgDataType getParameterRowType( Node left );


    void attachAnalyzer( Statement statement, AlgRoot logicalRoot ) {
        InformationManager queryAnalyzer = statement.getTransaction().getQueryAnalyzer();
        InformationPage page = new InformationPage( "Logical Query Plan" ).setLabel( "plans" );
        page.fullWidth();
        InformationGroup group = new InformationGroup( page, "Logical Query Plan" );
        queryAnalyzer.addPage( page );
        queryAnalyzer.addGroup( group );
        InformationQueryPlan informationQueryPlan = new InformationQueryPlan(
                group,
                AlgOptUtil.dumpPlan( "Logical Query Plan", logicalRoot.alg, ExplainFormat.JSON, ExplainLevel.ALL_ATTRIBUTES ) );
        queryAnalyzer.registerInformation( informationQueryPlan );
    }

}
