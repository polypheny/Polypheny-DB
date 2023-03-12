/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.polyfier.core;

import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Triple;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.processing.Processor;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.Pair;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Getter
@Slf4j
public class PolyfierQueryExecutor {
    private final Statement statement;
    private final AlgNode algNode;
    private final PolyfierQueryResult.PolyfierQueryResultBuilder resultBuilder;
    private final StopWatch queryExecutorTime;

    public PolyfierQueryExecutor( Triple<Statement, AlgNode, Long> data ) {
        this.statement = data.getLeft();
        this.algNode = data.getMiddle();
        this.resultBuilder = PolyfierQueryResult.builder().seed( data.getRight() );
        this.queryExecutorTime = StopWatch.create();
    }


    public PolyfierResult getResult() {
        return new PolyfierResult( this.resultBuilder.build() );
    }


    public PolyfierQueryExecutor execute() {
        this.queryExecutorTime.reset();
        this.queryExecutorTime.start();
        AlgRoot logicalRoot;
        try {
            if (log.isDebugEnabled()) {
                log.debug("Creating Root...");
            }
            logicalRoot = AlgRoot.of(algNode, Kind.SELECT);
        } catch (Exception | Error e) {
            resultBuilder.cause(e).success(false);
            this.queryExecutorTime.stop();
            return this;
        }
        resultBuilder.logical(logicalRoot.alg);

        PolyImplementation polyImplementation = null;
        AlgNode physicalRoot = null;
        try {
            if (log.isDebugEnabled()) {
                log.debug("Preparing Query...");
            }
            polyImplementation = statement.getQueryProcessor().prepareQuery(logicalRoot, true);
        } catch (Exception | Error e) {
            if (log.isDebugEnabled()) {
                log.error("Error occurred preparing Query. ", e);
            }
            resultBuilder.cause(e).success(false);
        } finally {
            if (log.isDebugEnabled()) {
                log.debug("Retrieving Physical Plan...");
            }
            physicalRoot = statement.getQueryProcessor().getPlanner().getRoot();
        }

        if ( physicalRoot == null || polyImplementation == null ) {
            resultBuilder.success(false);
            this.queryExecutorTime.stop();
            return this;
        }

        Iterator<Object> iterator;
        try {
            assert resultBuilder.physical != null;
            iterator = PolyImplementation.enumerable(polyImplementation.getBindable(), statement.getDataContext()).iterator();
        } catch (Exception | Error e) {
            resultBuilder.cause(e).success(false);
            this.queryExecutorTime.stop();
            return this;
        }

        StopWatch stopWatch = null;
        List<List<Object>> result = null;

        try {
            if (log.isDebugEnabled()) {
                log.debug("Executing..");
            }
            stopWatch = new StopWatch();
            stopWatch.start();
            result = MetaImpl.collect(polyImplementation.getCursorFactory(), iterator, new ArrayList<>());
            stopWatch.stop();
            polyImplementation.getExecutionTimeMonitor().setExecutionTime(stopWatch.getTime(TimeUnit.NANOSECONDS));
            resultBuilder.actualTime(stopWatch.getTime(TimeUnit.MILLISECONDS)).resultSet(result).success(true);
        } catch (Exception | Error e) {
            if (log.isDebugEnabled()) {
                log.error("Error occurred executing Query. ", e);
            }
            assert stopWatch != null;
            stopWatch.stop();
            resultBuilder.cause(e).message(e.getMessage()).success(false).actualTime(stopWatch.getTime(TimeUnit.MILLISECONDS));
            if (result != null) {
                resultBuilder.resultSet(result).success(true);
            }
        }

        this.queryExecutorTime.stop();

        return this;
    }


    /**
     * // Todo remove
     */
    public static List<List<Object>> auxiliaryExecute( Statement statement, String sql ) {
        Processor sqlProcessor = statement.getTransaction().getProcessor( QueryLanguage.from( "sql" ) );
        Pair<Node, AlgDataType> validated = sqlProcessor.validate( statement.getTransaction(), sqlProcessor.parse( sql ).get( 0 ), false );
        AlgRoot logicalRoot = sqlProcessor.translate( statement, validated.left, new QueryParameters( sql, Catalog.NamespaceType.RELATIONAL ) );
        PolyImplementation polyImplementation = statement.getQueryProcessor().prepareQuery( logicalRoot, false );

        List<List<Object>> result = null;
        try {
            result = MetaImpl.collect( polyImplementation.getCursorFactory(), PolyImplementation.enumerable( polyImplementation.getBindable() , statement.getDataContext() ).iterator(), new ArrayList<>() );
        } catch ( Exception e ) {
            throw new PolyfierException( "Could not execute insert query", e );
        }
        return result;
    }

    @Getter(AccessLevel.PUBLIC)
    @Builder(access = AccessLevel.PRIVATE)
    public static class PolyfierQueryResult {
        private AlgNode logical;
        private AlgNode physical;
        private List<List<Object>> resultSet;
        private Long actualTime;
        private Throwable cause;
        private String message;
        private Long predictedTime;
        private Long seed;
        private Boolean success;

        public Optional<AlgNode> getLogical() {
            if ( logical == null) {
                return Optional.empty();
            }
            return Optional.of( logical );
        }

        public Optional<AlgNode> getPhysical() {
            if ( physical == null) {
                return Optional.empty();
            }
            return Optional.of( physical );
        }

        public Optional<List<List<Object>>> getResultSet() {
            if ( resultSet == null) {
                return Optional.empty();
            }
            return Optional.of( resultSet );
        }

        public Optional<Long> getActualTime() {
            if ( actualTime == null) {
                return Optional.empty();
            }
            return Optional.of( actualTime );
        }

        public Optional<Throwable> getCause() {
            if ( cause == null) {
                return Optional.empty();
            }
            return Optional.of( cause );
        }

        public Optional<String> getMessage() {
            if ( message == null) {
                return Optional.empty();
            }
            return Optional.of( message );
        }

        public Optional<Long> getPredictedTime() {
            if ( predictedTime == null) {
                return Optional.empty();
            }
            return Optional.of( predictedTime );
        }

        public Optional<Long> getSeed() {
            if ( seed == null) {
                return Optional.empty();
            }
            return Optional.of( seed );
        }

        public Optional<Boolean> getSuccess() {
            if ( success == null) {
                return Optional.empty();
            }
            return Optional.of( success );
        }

    }




}
