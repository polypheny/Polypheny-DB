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

package org.polypheny.db.piglet;

import com.google.common.collect.ImmutableList;
import java.io.StringReader;
import java.util.Arrays;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.piglet.Ast.PigNode;
import org.polypheny.db.piglet.Ast.Program;
import org.polypheny.db.piglet.parser.ParseException;
import org.polypheny.db.piglet.parser.PigletParser;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.tools.PigAlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.util.DeadlockException;
import org.polypheny.db.util.Pair;

@Slf4j
public class PigProcessor extends Processor {

    private String query;


    @Override
    public List<? extends Node> parse( String query ) {
        this.query = query.trim();
        final StopWatch stopWatch = new StopWatch();
        if ( log.isDebugEnabled() ) {
            log.debug( "Parsing PolyPIG statement ..." );
        }
        stopWatch.start();
        Program parsed;
        if ( log.isDebugEnabled() ) {
            log.debug( "PIG: {}", query );
        }

        try {
            if ( !query.endsWith( ";" ) ) {
                query = query + ";";
            }
            parsed = new PigletParser( new StringReader( query ) ).stmtListEof();
        } catch ( ParseException e ) {
            log.error( "Caught exception", e );
            throw new GenericRuntimeException( e );
        }
        stopWatch.stop();
        if ( log.isTraceEnabled() ) {
            log.trace( "Parsed query: [{}]", parsed );
        }
        if ( log.isDebugEnabled() ) {
            log.debug( "Parsing PolyPIG statement ... done. [{}]", stopWatch );
        }
        return ImmutableList.of( parsed );
    }


    @Override
    public Pair<Node, AlgDataType> validate( Transaction transaction, Node parsed, boolean addDefaultValues ) {
        throw new GenericRuntimeException( "The PIG implementation does not support validation." );
    }


    @Override
    public AlgRoot translate( Statement statement, ParsedQueryContext context ) {
        final PigAlgBuilder builder = PigAlgBuilder.create( statement );
        new Handler( builder ).handle( (PigNode) context.getQueryNode().orElseThrow() );
        return AlgRoot.of( builder.build(), Kind.SELECT );
    }


    @Override
    public void unlock( Statement statement ) {
        throw new GenericRuntimeException( "The PIG implementation does not support DDL or DML operations and can therefore not lock or unlock." );
    }


    @Override
    public void lock( Statement statement ) throws DeadlockException {
        throw new GenericRuntimeException( "The PIG implementation does not support DDL or DML operations and can therefore not lock or unlock." );
    }


    @Override
    public String getQuery( Node parsed, QueryParameters parameters ) {
        return query;
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
