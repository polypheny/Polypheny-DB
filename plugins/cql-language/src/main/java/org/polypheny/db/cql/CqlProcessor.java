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

package org.polypheny.db.cql;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.cql.parser.CqlParser;
import org.polypheny.db.cql.parser.ParseException;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Lock.LockMode;
import org.polypheny.db.transaction.LockManager;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionImpl;
import org.polypheny.db.util.DeadlockException;
import org.polypheny.db.util.Pair;

public class CqlProcessor extends Processor {

    @Override
    public List<? extends Node> parse( String query ) {
        CqlParser cqlParser = new CqlParser( query, Catalog.DATABASE_NAME );
        try {
            return List.of( cqlParser.parse() );
        } catch ( ParseException e ) {
            throw new GenericRuntimeException( e );
        }
    }


    @Override
    public Pair<Node, AlgDataType> validate( Transaction transaction, Node parsed, boolean addDefaultValues ) {
        return null;
    }


    @Override
    public AlgRoot translate( Statement statement, ParsedQueryContext context ) {
        AlgBuilder algBuilder = AlgBuilder.create( statement );
        JavaTypeFactory typeFactory = statement.getTransaction().getTypeFactory();
        RexBuilder rexBuilder = new RexBuilder( typeFactory );

        Cql2AlgConverter cql2AlgConverter = new Cql2AlgConverter( (CqlQuery) context.getQueryNode().orElseThrow() );
        return cql2AlgConverter.convert2Alg( algBuilder, rexBuilder );
    }


    @Override
    public void unlock( Statement statement ) {
        LockManager.INSTANCE.unlock( Collections.singletonList( LockManager.GLOBAL_LOCK ), (TransactionImpl) statement.getTransaction() );
    }


    @Override
    protected void lock( Statement statement ) throws DeadlockException {
        LockManager.INSTANCE.lock( Collections.singletonList( Pair.of( LockManager.GLOBAL_LOCK, LockMode.EXCLUSIVE ) ), (TransactionImpl) statement.getTransaction() );
    }


    @Override
    public String getQuery( Node parsed, QueryParameters parameters ) {
        return parsed.toString();
    }


    @Override
    public AlgDataType getParameterRowType( Node left ) {
        return null;
    }


    @Override
    public List<String> splitStatements( String statements ) {
        return Arrays.stream( statements.split( ";" ) ).filter( q -> !q.trim().isEmpty() ).toList();
    }

}
