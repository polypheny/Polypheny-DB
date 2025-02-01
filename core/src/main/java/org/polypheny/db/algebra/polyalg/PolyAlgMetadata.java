/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.algebra.polyalg;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashMap;
import java.util.Map;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgOptCost;

public class PolyAlgMetadata {

    private final GlobalStats globalStats;
    private final ObjectMapper mapper;

    private final ArrayNode table; // simple key-value pairs
    private final ArrayNode badges; // can be used for indicating max values etc.
    private final ObjectNode outConnection; // width < 0: use default width, otherwise between 0 (min) and 1 (max)


    public PolyAlgMetadata( ObjectMapper mapper, GlobalStats globalStats ) {
        this.mapper = mapper;
        this.globalStats = globalStats;
        this.table = mapper.createArrayNode();
        this.badges = mapper.createArrayNode();
        this.outConnection = mapper.createObjectNode();
    }


    public PolyAlgMetadata addCosts( AlgOptCost nonCumulative, AlgOptCost cumulative, double tupleCount ) {
        addMetadata( "tupleCount", "Tuple Count", tupleCount, false );
        addMetadata( "tuplesCost", "Processed Tuples", nonCumulative.getRows(), cumulative.getRows(), false, "Most Tuples Processed" );
        addMetadata( "cpuCost", "CPU Cost", nonCumulative.getCpu(), cumulative.getCpu(), false, "Most CPU" );
        setOutConnection( "tupleCount", false );
        return this;
    }


    public PolyAlgMetadata addMetadata( String key, String displayName, double value, boolean isCalculated ) {
        return addMetadata( key, displayName, value, -1, isCalculated, null );
    }


    /**
     * Adds the given value to the metadata table.
     *
     * @param key unique key to identify the metadata
     * @param displayName name that is displayed instead of the key
     * @param value non-rounded value (non-cumulative)
     * @param cumulativeValue cumulative value or -1 if no such value exists
     * @param isCalculated whether the value is
     * @param displayMaxName display name of the badge to be shown if this is the highest global value or null if no badge should be shown
     * @return the PolyAlgMetadata for fluent chaining
     */
    public PolyAlgMetadata addMetadata( String key, String displayName, double value, double cumulativeValue, boolean isCalculated, String displayMaxName ) {
        ObjectNode row = mapper.createObjectNode();
        row.put( "key", key );
        row.put( "displayName", displayName );
        row.put( "value", value );
        if ( cumulativeValue >= 0 ) {
            row.put( "cumulativeValue", cumulativeValue );
        }
        row.put( "calculated", isCalculated );
        table.add( row );

        if ( displayMaxName != null && globalStats.isMax( key, value ) ) {
            addBadge( displayMaxName, key, BadgeLevel.DANGER );
        }
        return this;
    }


    /**
     * Sets the line width proportional to the value of this key divided by the global maximum.
     *
     * @param key the key of the stat to be used
     * @return the PolyAlgMetadata for fluent chaining
     */
    public PolyAlgMetadata setOutConnection( String key, boolean useCumulative ) {
        double max = useCumulative ? globalStats.getCumulativeMax( key ) : globalStats.getMax( key );
        ObjectNode row = findTableEntry( key );
        if ( row != null && max > 1 && (!useCumulative || row.has( "cumulativeValue" )) ) {
            double v = useCumulative ? row.get( "cumulativeValue" ).asDouble() : row.get( "value" ).asDouble();
            outConnection.put( "width", v / max );
            outConnection.put( "forKey", key );
        }
        return this;
    }


    public PolyAlgMetadata addBadge( String content, String forKey, BadgeLevel level ) {
        ObjectNode badge = mapper.createObjectNode();
        badge.put( "content", content );
        badge.put( "forKey", forKey );
        badge.put( "level", level.name() );
        badges.add( badge );
        return this;
    }


    public ObjectNode serialize() {
        ObjectNode node = mapper.createObjectNode();
        node.put( "isAuxiliary", false );
        node.set( "table", table );
        node.set( "badges", badges );

        if ( outConnection.has( "width" ) ) {
            node.set( "outConnection", outConnection );
        }
        return node;
    }


    public static ObjectNode getMetadataForAuxiliaryNode( ObjectMapper mapper ) {
        ObjectNode node = mapper.createObjectNode();
        node.put( "isAuxiliary", true );
        return node;
    }


    private ObjectNode findTableEntry( String key ) {
        for ( JsonNode node : table ) {
            if ( node.isObject() ) {
                ObjectNode row = (ObjectNode) node;
                if ( row.get( "key" ).asText().equals( key ) ) {
                    return row;
                }
            }
        }
        return null;
    }


    public static class GlobalStats {

        private final static double EPS = 0.0000001;
        private final Map<String, Double> maxValues = new HashMap<>();
        private final Map<String, Double> maxCumulativeValues = new HashMap<>();


        private GlobalStats() {
        }


        public static GlobalStats computeGlobalStats( AlgNode root ) {
            GlobalStats stats = new GlobalStats();
            stats.updateGlobalStats( root );
            stats.setMaxCumulativeCosts( root.getCluster().getMetadataQuery().getCumulativeCost( root ) );
            return stats;
        }


        private void updateGlobalStats( AlgNode node ) {
            for ( AlgNode child : node.getInputs() ) {
                updateGlobalStats( child );
            }
            AlgMetadataQuery mq = node.getCluster().getMetadataQuery();
            updateMaxCosts( mq.getNonCumulativeCost( node ), mq.getTupleCount( node ).orElse( -1D ) );
        }


        public void updateMaxCosts( AlgOptCost nonCumulative, Double tupleCount ) {
            update( "tupleCount", tupleCount );
            update( "tuplesCost", nonCumulative.getRows() );
            update( "cpuCost", nonCumulative.getCpu() );
        }


        public void setMaxCumulativeCosts( AlgOptCost cumulative ) {
            maxCumulativeValues.put( "tuplesCost", cumulative.getRows() );
            maxCumulativeValues.put( "cpuCost", cumulative.getCpu() );
        }


        public void update( String key, Double value ) {
            double curr = maxValues.getOrDefault( key, 0d );
            if ( value != null && value > curr ) {
                maxValues.put( key, value );
            }
        }


        public boolean isMax( String key, double value ) {
            return value > 0 && Math.abs( maxValues.getOrDefault( key, 0d ) - value ) < EPS;
        }


        public double getMax( String key ) {
            return maxValues.getOrDefault( key, 0d );
        }


        public double getCumulativeMax( String key ) {
            return maxCumulativeValues.getOrDefault( key, 0d );
        }

    }


    public enum BadgeLevel {
        INFO,
        WARN,
        DANGER
    }

}
