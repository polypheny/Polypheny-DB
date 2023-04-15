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
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.codec.digest.MurmurHash2;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.ExplainFormat;
import org.polypheny.db.algebra.constant.ExplainLevel;
import org.polypheny.db.plan.AlgOptUtil;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Mirrors the class PolyphenyDbRequest.Result in the Polyfier-Server module.
 */
@Getter
@Setter(AccessLevel.PRIVATE)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PolyfierResult implements Serializable {

    @Setter(AccessLevel.PUBLIC)
    public String apiKey;

    @Setter(AccessLevel.PUBLIC)
    public String orderKey;

    public Long seed;
    public Long resultSetHash;
    public Boolean success;
    public String error;
    public String logical;
    public String physical;
    public Long actual;
    public Long predicted;

    public boolean wasSuccess() {
        return this.success;
    }

    public PolyfierResult( PolyfierQueryExecutor.PolyfierQueryResult polyfierQueryResult ) {
        polyfierQueryResult.getLogical().ifPresentOrElse(
                node -> setLogical( formatLogicalPlan( node ) ),
                () -> setLogical( null )
        );
        polyfierQueryResult.getPhysical().ifPresentOrElse(
                node -> setPhysical( formatPhysicalPlan( node ) ),
                () -> setPhysical( null )
        );
        polyfierQueryResult.getCause().ifPresentOrElse(
                e -> setError( formatError( e ) ),
                () -> setError( null )
        );
        polyfierQueryResult.getActualTime().ifPresentOrElse(
                this::setActual,
                () -> setActual( null )
        );
        polyfierQueryResult.getPredictedTime().ifPresentOrElse(
                this::setPredicted,
                () -> setPredicted( null )
        );
        polyfierQueryResult.getSeed().ifPresentOrElse(
                this::setSeed,
                () -> setSeed( null )
        );
        polyfierQueryResult.getSuccess().ifPresentOrElse(
                this::setSuccess,
                () -> setSuccess( null )
        );
        polyfierQueryResult.getResultSet().ifPresentOrElse(
                resultSet -> setResultSetHash( rowOrderIndependentHash( resultSet ) ),
                () -> setResultSetHash( null )
        );

    }

    private String formatLogicalPlan( AlgNode node ) {
        return AlgOptUtil.dumpPlan("logicalPlan", node, ExplainFormat.TEXT, ExplainLevel.NON_COST_ATTRIBUTES );
    }

    private String formatPhysicalPlan( AlgNode node ) {
        return AlgOptUtil.dumpPlan("physicalPlan", node, ExplainFormat.TEXT, ExplainLevel.NON_COST_ATTRIBUTES );
    }

    private String formatError( Throwable cause ) {
        return "\n" + cause.toString() + "\n" + (( cause.getCause() == null ) ? "" : cause.getCause().getMessage() + "\n" + cause.getMessage());
    }

    private Long rowOrderIndependentHash( List<List<Object>> resultSet ) {
        if ( resultSet == null || resultSet.isEmpty() || resultSet.get( 0 ).isEmpty() ) {
            this.setSuccess( false );
            return null;
        }
        List<Long> hR = resultSet.stream().map( xs -> MurmurHash2.hash64( xs.toString() ) ).collect( Collectors.toList() );
        List<Long> hU = hR.stream().filter( x -> Collections.frequency( hR, x ) == 1 ).collect( Collectors.toList() );
        List<Long> hD = hR.stream().filter( x -> ! hU.contains( x ) ).distinct().collect( Collectors.toList() );
        hU.addAll( hD.stream().map( x -> Math.floorDiv( x, Collections.frequency( hR, x ) ) ).collect( Collectors.toList()) );
        return hU.stream().reduce( ( Long a, Long b ) -> a ^ b ).orElseThrow();
    }

    public static PolyfierResult blankFailure( long seed ) {
        PolyfierResult polyfierResult = new PolyfierResult();
        polyfierResult.setSuccess( false );
        polyfierResult.setError( "QueryGenerator" );
        polyfierResult.setSeed( seed );
        return polyfierResult;
    }

    @Override
    public String toString() {
        return "seed" + ":" + seed + "\n" +
                "time_m" + ":" + actual + "\n" +
                "time_p" + ":" + predicted + "\n" +
                "success" + ":" + success + "\n" +
                "error" + ":" + error + "\n" +
                "result-hash" + ":" + resultSetHash + "\n" +
                "----" + ":" + "----" + "\n" +
                "Logical Plan" + "\n" +
                logical + ":" + seed + "\n" +
                "Physical Plan" + "\n" +
                physical;
    }

}
