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

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.cypher.CypherNode;
import org.polypheny.db.cypher.parser.CypherParser;
import org.polypheny.db.cypher.parser.CypherParser.CypherParserConfig;
import org.polypheny.db.languages.NodeParseException;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.transaction.Lock.LockMode;
import org.polypheny.db.transaction.LockManager;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionImpl;
import org.polypheny.db.util.DeadlockException;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.SourceStringReader;

@Slf4j
public class CypherProcessorImpl extends Processor {

    private static final CypherParserConfig parserConfig;


    static {
        CypherParser.ConfigBuilder configConfigBuilder = CypherParser.configBuilder();
        parserConfig = configConfigBuilder.build();
    }


    @Override
    public Node parse( String query ) {
        final StopWatch stopWatch = new StopWatch();
        if ( log.isDebugEnabled() ) {
            log.debug( "Parsing PolyMQL statement ..." );
        }
        stopWatch.start();
        CypherNode parsed;
        if ( log.isDebugEnabled() ) {
            log.debug( "CYPHER: {}", query );
        }

        try {
            final CypherParser parser = CypherParser.create( new SourceStringReader( query ), parserConfig );
            parsed = parser.parseStmt();
        } catch ( NodeParseException e ) {
            log.error( "Caught exception", e );
            throw new RuntimeException( e );
        }
        stopWatch.stop();
        if ( log.isTraceEnabled() ) {
            log.trace( "Parsed query: [{}]", parsed );
        }
        if ( log.isDebugEnabled() ) {
            log.debug( "Parsing PolyCypher statement ... done. [{}]", stopWatch );
        }
        return parsed;
    }


    @Override
    public Pair<Node, AlgDataType> validate( Transaction transaction, Node parsed, boolean addDefaultValues ) {
        throw new RuntimeException( "The MQL implementation does not support validation." );
    }


    @Override
    public AlgRoot translate( Statement statement, Node query, QueryParameters parameters ) {
        /*
        final StopWatch stopWatch = new StopWatch();
        if ( log.isDebugEnabled() ) {
            log.debug( "Planning Statement ..." );
        }
        stopWatch.start();

        final RexBuilder rexBuilder = new RexBuilder( statement.getTransaction().getTypeFactory() );
        final AlgOptCluster cluster = AlgOptCluster.create( statement.getQueryProcessor().getPlanner(), rexBuilder );

        final CypherToAlgConverter cypherToAlgConverter = new CypherToAlgConverter( this, statement.getTransaction().getCatalogReader(), cluster );
        AlgRoot logicalRoot = cypherToAlgConverter.convert( query, parameters );

        // Decorrelate
        final AlgBuilder algBuilder = AlgBuilder.create( statement );
        logicalRoot = logicalRoot.withAlg( AlgDecorrelator.decorrelateQuery( logicalRoot.alg, algBuilder ) );

        if ( log.isTraceEnabled() ) {
            log.trace( "Logical query plan: [{}]", AlgOptUtil.dumpPlan( "-- Logical Plan", logicalRoot.alg, ExplainFormat.TEXT, ExplainLevel.DIGEST_ATTRIBUTES ) );
        }
        stopWatch.stop();
        if ( log.isDebugEnabled() ) {
            log.debug( "Planning Statement ... done. [{}]", stopWatch );
        }

        return logicalRoot;
         */
        return null;
    }


    @Override
    public void unlock( Statement statement ) {
        LockManager.INSTANCE.unlock( LockManager.GLOBAL_LOCK, (TransactionImpl) statement.getTransaction() );
    }


    @Override
    protected void lock( Statement statement ) throws DeadlockException {
        LockManager.INSTANCE.lock( LockManager.GLOBAL_LOCK, (TransactionImpl) statement.getTransaction(), LockMode.EXCLUSIVE );
    }


    @Override
    public String getQuery( Node parsed, QueryParameters parameters ) {
        return parameters.getQuery();
    }


    @Override
    public AlgDataType getParameterRowType( Node left ) {
        return null;
    }

}
