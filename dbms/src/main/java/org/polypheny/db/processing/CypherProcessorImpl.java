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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.algebra.AlgDecorrelator;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.ExplainFormat;
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.cypher.CypherNode;
import org.polypheny.db.cypher.CypherNode.CypherKind;
import org.polypheny.db.cypher.CypherNode.CypherVisitor;
import org.polypheny.db.cypher.CypherStatement;
import org.polypheny.db.cypher.clause.CypherCreate;
import org.polypheny.db.cypher.cypher2alg.CypherQueryParameters;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter;
import org.polypheny.db.cypher.parser.CypherParser;
import org.polypheny.db.cypher.parser.CypherParser.CypherParserConfig;
import org.polypheny.db.cypher.pattern.CypherEveryPathPattern;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.NodeParseException;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.transaction.Lock.LockMode;
import org.polypheny.db.transaction.LockManager;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionImpl;
import org.polypheny.db.util.DeadlockException;
import org.polypheny.db.util.Pair;

@Slf4j
public class CypherProcessorImpl extends AutomaticDdlProcessor {

    private static final CypherParserConfig parserConfig;


    static {
        CypherParser.ConfigBuilder configConfigBuilder = CypherParser.configBuilder();
        parserConfig = configConfigBuilder.build();
    }


    @Override
    public List<CypherStatement> parse( String query ) {
        final StopWatch stopWatch = new StopWatch();
        if ( log.isDebugEnabled() ) {
            log.debug( "Parsing PolyMQL statement ..." );
        }
        stopWatch.start();
        List<CypherStatement> parsed;
        if ( log.isDebugEnabled() ) {
            log.debug( "CYPHER: {}", query );
        }

        try {
            final CypherParser parser = CypherParser.create( query, parserConfig );
            parsed = parser.parseStmts();
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

        final StopWatch stopWatch = new StopWatch();
        if ( log.isDebugEnabled() ) {
            log.debug( "Planning Statement ..." );
        }
        stopWatch.start();

        final AlgBuilder builder = AlgBuilder.create( statement );
        final RexBuilder rexBuilder = new RexBuilder( statement.getTransaction().getTypeFactory() );
        final AlgOptCluster cluster = AlgOptCluster.createGraph( statement.getQueryProcessor().getPlanner(), rexBuilder );

        final CypherToAlgConverter cypherToAlgConverter = new CypherToAlgConverter( statement, builder, rexBuilder, cluster );

        AlgRoot logicalRoot = cypherToAlgConverter.convert( (CypherNode) query, (CypherQueryParameters) parameters, cluster );

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
        LockManager.INSTANCE.unlock( List.of( LockManager.GLOBAL_LOCK ), (TransactionImpl) statement.getTransaction() );
    }


    @Override
    protected void lock( Statement statement ) throws DeadlockException {
        LockManager.INSTANCE.lock( List.of( Pair.of( LockManager.GLOBAL_LOCK, LockMode.EXCLUSIVE ) ), (TransactionImpl) statement.getTransaction() );
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
    public void autoGenerateDDL( Statement statement, Node node, QueryParameters parameters ) {

        assert parameters instanceof CypherQueryParameters;
        try {
            DdlManager ddlManager = DdlManager.getInstance();
            long namespaceId = ddlManager.createGraph(
                    Catalog.defaultDatabaseId, ((CypherQueryParameters) parameters).getDatabaseName(), true, null, true, false, statement );

            statement.getTransaction().commit();
            ((CypherQueryParameters) parameters).setDatabaseId( namespaceId );
        } catch ( TransactionException e ) {
            throw new RuntimeException( "Unable to commit auto-generated structure:" + e.getMessage() );
        }
    }


    @Override
    public boolean needsDdlGeneration( Node query, QueryParameters parameters ) {
        return false;
    }


    @Getter
    public static class LabelExtractor extends CypherVisitor {

        final List<String> nodeLabels = new ArrayList<>();
        final List<String> relationshipLabels = new ArrayList<>();


        @Override
        public void visit( CypherCreate create ) {

            List<CypherEveryPathPattern> paths = create
                    .getPatterns()
                    .stream()
                    .filter( p -> p.getCypherKind() == CypherKind.PATH )
                    .map( p -> (CypherEveryPathPattern) p )
                    .collect( Collectors.toList() );

            relationshipLabels.addAll( paths
                    .stream()
                    .map( CypherEveryPathPattern::getEdges )
                    .flatMap( rs -> rs.stream().flatMap( r -> r.getLabels().stream() ) )
                    .collect( Collectors.toSet() ) );

            nodeLabels.addAll( paths
                    .stream()
                    .map( CypherEveryPathPattern::getNodes )
                    .flatMap( rs -> rs.stream().flatMap( n -> n.getLabels().stream() ) )
                    .collect( Collectors.toSet() ) );

            super.visit( create );
        }

    }

}
