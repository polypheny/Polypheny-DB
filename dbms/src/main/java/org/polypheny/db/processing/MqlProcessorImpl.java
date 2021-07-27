/*
 * Copyright 2019-2021 The Polypheny Project
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.Meta;
import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.exceptions.NoTablePrimaryKeyException;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationQueryPlan;
import org.polypheny.db.jdbc.PolyphenyDbSignature;
import org.polypheny.db.mql.MqlExecutableStatement;
import org.polypheny.db.mql.MqlNode;
import org.polypheny.db.mql.parser.MqlParseException;
import org.polypheny.db.mql.parser.MqlParser;
import org.polypheny.db.mql.parser.MqlParser.MqlParserConfig;
import org.polypheny.db.mql2rel.MqlToRelConverter;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptTable.ViewExpander;
import org.polypheny.db.plan.RelOptUtil;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.routing.ExecutionTimeMonitor;
import org.polypheny.db.sql.SqlExplainFormat;
import org.polypheny.db.sql.SqlExplainLevel;
import org.polypheny.db.sql2rel.RelDecorrelator;
import org.polypheny.db.tools.RelBuilder;
import org.polypheny.db.transaction.DeadlockException;
import org.polypheny.db.transaction.Lock.LockMode;
import org.polypheny.db.transaction.LockManager;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.TransactionException;
import org.polypheny.db.transaction.TransactionImpl;
import org.polypheny.db.util.SourceStringReader;

@Slf4j
public class MqlProcessorImpl implements MqlProcessor, ViewExpander {

    private static final MqlParserConfig parserConfig;


    static {
        MqlParser.ConfigBuilder configConfigBuilder = MqlParser.configBuilder();
        parserConfig = configConfigBuilder.build();
    }


    @Override
    public RelRoot expandView( RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath ) {
        return null;
    }


    @Override
    public MqlNode parse( String mql ) {
        final StopWatch stopWatch = new StopWatch();
        if ( log.isDebugEnabled() ) {
            log.debug( "Parsing PolyMQL statement ..." );
        }
        stopWatch.start();
        MqlNode parsed;
        log.debug( "MQL: {}", mql );

        try {
            final MqlParser parser = MqlParser.create( new SourceStringReader( mql ), parserConfig );
            parsed = parser.parseStmt();
        } catch ( MqlParseException e ) {
            log.error( "Caught exception", e );
            throw new RuntimeException( e );
        }
        stopWatch.stop();
        if ( log.isTraceEnabled() ) {
            log.trace( "Parsed query: [{}]", parsed );
        }
        if ( log.isDebugEnabled() ) {
            log.debug( "Parsing PolyMQL statement ... done. [{}]", stopWatch );
        }
        return parsed;
    }


    @Override
    public void validate( RelRoot root, boolean addDefaultValues ) {
        return;
    }


    @Override
    public RelRoot translate( Statement statement, MqlNode mql, String defaultDatabase ) {
        final StopWatch stopWatch = new StopWatch();
        if ( log.isDebugEnabled() ) {
            log.debug( "Planning Statement ..." );
        }
        stopWatch.start();

        final RexBuilder rexBuilder = new RexBuilder( statement.getTransaction().getTypeFactory() );
        final RelOptCluster cluster = RelOptCluster.create( statement.getQueryProcessor().getPlanner(), rexBuilder );

        final MqlToRelConverter mqlToRelConverter = new MqlToRelConverter( this, statement.getTransaction().getCatalogReader(), cluster );
        RelRoot logicalRoot = mqlToRelConverter.convert( mql, false, true, defaultDatabase );


        if ( statement.getTransaction().isAnalyze() ) {
            InformationManager queryAnalyzer = statement.getTransaction().getQueryAnalyzer();
            InformationPage page = new InformationPage( "Logical Query Plan" ).setLabel( "plans" );
            page.fullWidth();
            InformationGroup group = new InformationGroup( page, "Logical Query Plan" );
            queryAnalyzer.addPage( page );
            queryAnalyzer.addGroup( group );
            InformationQueryPlan informationQueryPlan = new InformationQueryPlan(
                    group,
                    RelOptUtil.dumpPlan( "Logical Query Plan", logicalRoot.rel, SqlExplainFormat.JSON, SqlExplainLevel.ALL_ATTRIBUTES ) );
            queryAnalyzer.registerInformation( informationQueryPlan );
        }

        // Decorrelate
        final RelBuilder relBuilder = RelBuilder.create( statement );
        logicalRoot = logicalRoot.withRel( RelDecorrelator.decorrelateQuery( logicalRoot.rel, relBuilder ) );


        if ( log.isTraceEnabled() ) {
            log.trace( "Logical query plan: [{}]", RelOptUtil.dumpPlan( "-- Logical Plan", logicalRoot.rel, SqlExplainFormat.TEXT, SqlExplainLevel.DIGEST_ATTRIBUTES ) );
        }
        stopWatch.stop();
        if ( log.isDebugEnabled() ) {
            log.debug( "Planning Statement ... done. [{}]", stopWatch );
        }

        return logicalRoot;
    }


    @Override
    public PolyphenyDbSignature<?> prepareDdl( Statement statement, MqlNode parsed, String mql ) {
        if ( parsed instanceof MqlExecutableStatement ) {
            try { // TODO DL merge with sql processor
                // Acquire global schema lock
                LockManager.INSTANCE.lock( LockManager.GLOBAL_LOCK, (TransactionImpl) statement.getTransaction(), LockMode.EXCLUSIVE );
                // Execute statement
                ((MqlExecutableStatement) parsed).execute( statement.getPrepareContext(), statement );
                statement.getTransaction().commit();
                Catalog.getInstance().commit();
                return new PolyphenyDbSignature<>(
                        mql,
                        ImmutableList.of(),
                        ImmutableMap.of(),
                        null,
                        ImmutableList.of(),
                        Meta.CursorFactory.OBJECT,
                        statement.getTransaction().getSchema(),
                        ImmutableList.of(),
                        -1,
                        null,
                        Meta.StatementType.OTHER_DDL,
                        new ExecutionTimeMonitor() );
            } catch ( DeadlockException e ) {
                throw new RuntimeException( "Exception while acquiring global schema lock", e );
            } catch ( TransactionException | NoTablePrimaryKeyException e ) {
                throw new RuntimeException( e );
            } finally {
                // Release lock
                LockManager.INSTANCE.unlock( LockManager.GLOBAL_LOCK, (TransactionImpl) statement.getTransaction() );
            }
        } else {
            throw new RuntimeException( "All DDL queries should be of a type that inherits SqlExecutableStatement. But this one is of type " + parsed.getClass() );
        }
    }


    @Override
    public RelDataType getParameterRowType( MqlNode left ) {
        return null;
    }

}
