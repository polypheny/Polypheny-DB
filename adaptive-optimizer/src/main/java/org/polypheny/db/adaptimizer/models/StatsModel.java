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

package org.polypheny.db.adaptimizer.models;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.plan.PhysicalPlan;

@Slf4j
public class StatsModel implements Model {
    private static final HashMap<String, List<Double>> dOW; // operator weight changes
    private static final HashMap<String, Double> oW; // operator weights

    static {
        oW = new HashMap<>();
        dOW = new HashMap<>();
    }

    @Override
    public long estimate( PhysicalPlan physicalPlan ) {
        return (long) auxiliaryEstimate( physicalPlan.getRoot() ).doubleValue();
    }

    @Override
    public void process( PhysicalPlan physicalPlan ) {
        double actual = physicalPlan.getActualExecutionTime();
        double estimate = physicalPlan.getEstimatedExecutionTime();
        log.debug( "Distance: {}", ( actual - estimate ) );
        auxiliaryProcess(
                (Map<String, Object>) ((Map<String, Object>)new Gson().fromJson( physicalPlan.getJsonAlg(), new TypeToken<HashMap<String, Object>>() {}.getType() )).get( "Plan" ),
                actual,
                estimate
        );
    }

    @Override
    public void update() {
        dOW.forEach( ( k, v ) -> oW.put( k, oW.get( k ) + ( v.stream().mapToDouble( Double::doubleValue ).sum() / Math.max( v.size(), 1d ) ) ) );
        dOW.values().forEach( List::clear );
        log.debug( new GsonBuilder().serializeSpecialFloatingPointValues().setPrettyPrinting().create().toJson( oW ) );
    }

    private void auxiliaryProcess( Map<String, Object> node, double actual, double estimate ) {
        List<Map<String, Object>> inputs = ((List<Map<String, Object>>) node.get( "inputs" ));
        String key;
        try {
            key = node.get( "relOp" ).toString();
//                log.debug( "OPERATOR: {}", key );
        } catch ( NullPointerException exception ) {
            log.debug( "{} has no relOp", node );
            return;
        }
        //log.debug( "Processing: {}", key );
        Double w = oW.get( node.get( "relOp" ).toString() );

        // Todo calculate change in weight.

        double curr = estimate - w;
        curr = ( curr >= 0 ) ? Math.max( curr, 1 ) : Math.min( -1, curr );
        double diff = actual - curr;
        double wNew = 0.01 * ( w / curr ) * diff;

        if ( Math.abs( wNew ) >= actual ) {
            wNew /= actual;
        }

//            log.debug( "FUNCTION: " );
//            log.debug( " {} = estimate ", estimate );
//            log.debug( " {} = actual ", actual );
//            log.debug( " {} = estimate - w;}", curr );
//            log.debug( " {} = ( curr <= 1 ) ? 1 : curr;", curr );
//            log.debug( " {} = actual - curr;}", diff );
//            log.debug( " {} = curr + (( diff > 0) ? 1 : -1 ) * ( w / curr ) * diff", wNew );

        switch ( inputs.size() ) {
            case 0:
                //log.debug( "0 input ordinal: {}", wNew );
                dOW.get( key ).add( wNew );
                break;
            case 1:
                //log.debug( "1 input ordinal: {}", wNew );
                dOW.get( key ).add( wNew );
                this.auxiliaryProcess( inputs.get( 0 ), actual - wNew, curr );
                break;
            case 2:
                //log.debug( "2 input ordinal: {}", wNew );
                dOW.get( key ).add( wNew );
                this.auxiliaryProcess( inputs.get( 0 ), actual - wNew, curr );
                this.auxiliaryProcess( inputs.get( 1 ), actual - wNew, curr );
                break;
            default:
                break;
        }
    }

    private Double initializeWeight( final AlgNode node ) {
        switch ( node.getInputs().size() ) {
            case 0:
                AlgOptTable table = node.getTable();
                oW.put( node.getAlgTypeName(), ( table == null ) ? 0d : 1d );
                dOW.put( node.getAlgTypeName(), new LinkedList<>() );
                break;
            case 1:
            case 2:
                oW.put( node.getAlgTypeName(), 1d );
                dOW.put( node.getAlgTypeName(), new LinkedList<>() );
                break;
            default:
                throw new RuntimeException( "Unexpected Input Count" );
        }
        return oW.get( node.getAlgTypeName() );
    }

    private Double getWeight( final AlgNode node ) {
        if ( oW.containsKey( node.getAlgTypeName() ) ) {
            return oW.get( node.getAlgTypeName() );
        }
        return initializeWeight( node );
    }

    private Double weightOf( final AlgNode node ) {
        return getWeight( node );
    }

    private Double auxiliaryEstimate( final AlgNode node ) {
        //log.debug( "Estimating: {}", node.getAlgTypeName() );
        if ( node == null ) {
            return null;
        }
        Double w = weightOf( node );
        double carry = node.getInputs().stream().map( this::auxiliaryEstimate )
                .filter( estimate -> ! Objects.isNull( estimate ) ).mapToDouble( d -> d + w ).sum();
        if ( carry == 0 && node.getTable() != null ) {
            carry = w;
        }
        oW.put( node.getAlgTypeName(), w );
        return carry;
    }

}