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

package org.polypheny.db.languages;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.algebra.AlgDecorrelator;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.ExplainFormat;
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.languages.mql.MqlNode;
import org.polypheny.db.languages.mql.parser.MqlParser;
import org.polypheny.db.languages.mql.parser.MqlParser.MqlParserConfig;
import org.polypheny.db.languages.mql2alg.MqlToAlgConverter;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptUtil;
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
import org.polypheny.db.util.SourceStringReader;


@Slf4j
public class MqlProcessor extends Processor {

    private static final MqlParserConfig parserConfig;


    static {
        MqlParser.ConfigBuilder configConfigBuilder = MqlParser.configBuilder();
        parserConfig = configConfigBuilder.build();
    }


    @Override
    public List<? extends Node> parse( String mql ) {
        final StopWatch stopWatch = new StopWatch();
        if ( log.isDebugEnabled() ) {
            log.debug( "Parsing PolyMQL statement ..." );
        }
        stopWatch.start();
        MqlNode parsed;
        if ( log.isDebugEnabled() ) {
            log.debug( "MQL: {}", mql );
        }

        // preprocessing

        mql = preprocess( mql );

        try {
            final MqlParser parser = MqlParser.create( new SourceStringReader( mql ), parserConfig );
            parsed = parser.parseStmt();
        } catch ( NodeParseException e ) {
            log.error( "Caught exception", e );
            throw new GenericRuntimeException( e );
        }
        stopWatch.stop();
        if ( log.isTraceEnabled() ) {
            log.trace( "Parsed query: [{}]", parsed );
        }
        if ( log.isDebugEnabled() ) {
            log.debug( "Parsing PolyMQL statement ... done. [{}]", stopWatch );
        }
        return ImmutableList.of( parsed );
    }


    private String preprocess( String query ) {
        String lowercase = query.toLowerCase();
        if ( lowercase.startsWith( "use " ) || lowercase.startsWith( "show " ) || lowercase.startsWith( "db." ) ) {
            return query;
        }
        String[] splits = query.split( "\\." );
        if ( splits.length > 1 ) {
            // we prefix query "entity".command( with db."entity.command(" as this is simpler to parse
            if ( splits[1].contains( "(" ) && !(splits[1].startsWith( "create" ) || splits[1].startsWith( "drop" )) ) {
                return "db." + query;
            }
        }

        return query;
    }


    @Override
    public Pair<Node, AlgDataType> validate( Transaction transaction, Node parsed, boolean addDefaultValues ) {
        throw new GenericRuntimeException( "The MQL implementation does not support validation." );
    }


    @Override
    public AlgRoot translate( Statement statement, ParsedQueryContext context ) {
        final StopWatch stopWatch = new StopWatch();
        if ( log.isDebugEnabled() ) {
            log.debug( "Planning Statement ..." );
        }
        stopWatch.start();

        final RexBuilder rexBuilder = new RexBuilder( statement.getTransaction().getTypeFactory() );
        final AlgCluster cluster = AlgCluster.createDocument( statement.getQueryProcessor().getPlanner(), rexBuilder, statement.getTransaction().getSnapshot() );

        final MqlToAlgConverter mqlToAlgConverter = new MqlToAlgConverter( statement.getTransaction().getSnapshot(), cluster );
        AlgRoot logicalRoot = mqlToAlgConverter.convert( context );

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
    }


    @Override
    public void unlock( Statement statement ) {
        LockManager.INSTANCE.unlock( Collections.singletonList( LockManager.GLOBAL_LOCK ), (TransactionImpl) statement.getTransaction() );
    }


    @Override
    public void lock( Statement statement ) throws DeadlockException {
        LockManager.INSTANCE.lock( Collections.singletonList( Pair.of( LockManager.GLOBAL_LOCK, LockMode.EXCLUSIVE ) ), (TransactionImpl) statement.getTransaction() );
    }


    @Override
    public String getQuery( Node parsed, QueryParameters parameters ) {
        return parameters.getQuery();
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
