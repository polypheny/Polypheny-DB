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

package org.polypheny.db.adaptimizer.polyfierconnect;

import lombok.AccessLevel;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.avatica.MetaImpl;
import org.apache.commons.lang3.time.StopWatch;

import org.apache.commons.lang3.tuple.Triple;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.ExplainFormat;
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.PrecedenceClimbingParser;

import java.io.Serializable;
import java.util.*;

@Slf4j
public class PolyfierQueryExecutor {
    private final Statement statement;
    private final AlgNode algNode;
    private final PolyfierQueryResult.PolyfierQueryResultBuilder resultBuilder;

    public PolyfierQueryExecutor( Statement statement, AlgNode algNode, long seed ) {
        this.statement = statement;
        this.algNode = algNode;
        this.resultBuilder = PolyfierQueryResult.builder().seed( seed );
    }

    public PolyfierQueryExecutor( Triple<Statement, AlgNode, Long> data ) {
        this.statement = data.getLeft();
        this.algNode = data.getMiddle();
        this.resultBuilder = PolyfierQueryResult.builder().seed( data.getRight() );
    }


    public PolyfierResult getResult() {
        return new PolyfierResult( this.resultBuilder.build() );
    }


    public PolyfierQueryExecutor execute() {

        AlgRoot logicalRoot;
        try {
            logicalRoot = AlgRoot.of( algNode, Kind.SELECT );
        } catch ( Exception | Error e ) {
            if ( log.isDebugEnabled() ) {
                throw new RuntimeException( e );
            }
            resultBuilder.cause( e ).success( false );
            return this;
        }
        resultBuilder.logical( logicalRoot.alg );

        PolyImplementation polyImplementation = null;
        try {
            polyImplementation = statement.getQueryProcessor().prepareQuery( logicalRoot, true );
        } catch ( Exception | Error e ) {
            if ( log.isDebugEnabled() ) {
                log.error("Error occurred preparing Query. ");
                throw new RuntimeException( e );
            }
            resultBuilder.cause( e ).success( false );
        } finally {
            resultBuilder.physical( statement.getQueryProcessor().getPlanner().getRoot() );
        }

        if ( resultBuilder.cause != null || polyImplementation == null ) {
            return this;
        }

        Iterator<Object> iterator;
        try {
            iterator = PolyImplementation.enumerable( polyImplementation.getBindable() , statement.getDataContext() ).iterator();
        } catch ( Exception | Error e ) {
            if ( log.isDebugEnabled() ) {
                throw new RuntimeException( e );
            }
            resultBuilder.cause( e ).success( false );
            return this;
        }

        StopWatch stopWatch = null;
        List<List<Object>> result = null;
        try {
            stopWatch = new StopWatch();
            stopWatch.start();
            result = MetaImpl.collect( polyImplementation.getCursorFactory(), iterator, new ArrayList<>() );
            stopWatch.stop();
            polyImplementation.getExecutionTimeMonitor().setExecutionTime( stopWatch.getNanoTime() );
            resultBuilder.actualTime( stopWatch.getNanoTime() ).success( true );
        } catch ( Exception | Error e ) {
            log.error("Error occurred executing Query. ");
            if ( log.isDebugEnabled() ) {
                throw new RuntimeException( e );
            }
            assert stopWatch != null;
            stopWatch.stop();
            resultBuilder.cause( e ).success( false ).actualTime( stopWatch.getNanoTime() );
            if ( result != null ) {
                resultBuilder.resultSet( result ).success( true );
            }
        }
        return this;
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
