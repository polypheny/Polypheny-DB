/*
 * Copyright 2019-2020 The Polypheny Project
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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.information.InformationAction;
import org.polypheny.db.information.InformationGraph;
import org.polypheny.db.information.InformationGraph.GraphData;
import org.polypheny.db.information.InformationGraph.GraphType;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationKeyValue;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.information.InformationText;
import org.polypheny.db.rel.RelNode;

public class QueryPlanCache {

    public static final QueryPlanCache INSTANCE = new QueryPlanCache();

    private final Cache<String, RelNode> planCache;

    private final AtomicLong hitsCounter = new AtomicLong(); // Number of requests for which the cache contained the value
    private final AtomicLong missesCounter = new AtomicLong(); // Number of requests for which the cache hasn't contained the value


    public QueryPlanCache() {
        planCache = CacheBuilder.newBuilder()
                .maximumSize( RuntimeConfig.QUERY_PLAN_CACHING_SIZE.getInteger() )
                //  .expireAfterWrite(10, TimeUnit.MINUTES)
                .build();
        registerMonitoringPage();
    }


    public RelNode getIfPresent( RelNode parameterizedNode ) {
        RelNode node = planCache.getIfPresent( parameterizedNode.relCompareString() );
        if ( node == null ) {
            missesCounter.incrementAndGet();
        } else {
            hitsCounter.incrementAndGet();
        }
        return node;
    }


    public void put( RelNode parameterizedNode, RelNode optimalNode ) {
        planCache.put( parameterizedNode.relCompareString(), optimalNode );
    }


    public void reset() {
        planCache.invalidateAll();
        hitsCounter.set( 0 );
        missesCounter.set( 0 );
    }


    private void registerMonitoringPage() {
        InformationManager im = InformationManager.getInstance();

        InformationPage page = new InformationPage( "Query Plan Cache" );
        im.addPage( page );

        // General
        InformationGroup generalGroup = new InformationGroup( page, "General" ).setOrder( 1 );
        im.addGroup( generalGroup );

        InformationKeyValue generalKv = new InformationKeyValue( generalGroup );
        im.registerInformation( generalKv );
        generalGroup.setRefreshFunction( () -> {
            generalKv.putPair( "Status", RuntimeConfig.QUERY_PLAN_CACHING.getBoolean() ? "Active" : "Disabled" );
            generalKv.putPair( "Current Cache Size", planCache.size() + "" );
            generalKv.putPair( "Maximum Cache Size", RuntimeConfig.QUERY_PLAN_CACHING_SIZE.getInteger() + "" );
        } );

        // Hit ratio
        InformationGroup hitRatioGroup = new InformationGroup( page, "Hit Ratio" ).setOrder( 2 );
        im.addGroup( hitRatioGroup );

        InformationGraph hitInfoGraph = new InformationGraph(
                hitRatioGroup,
                GraphType.DOUGHNUT,
                new String[]{ "Hits", "Misses" }
        );
        hitInfoGraph.setOrder( 1 );
        im.registerInformation( hitInfoGraph );

        InformationTable hitInfoTable = new InformationTable(
                hitRatioGroup,
                Arrays.asList( "Attribute", "Percent", "Absolute" )
        );
        hitInfoTable.setOrder( 2 );
        im.registerInformation( hitInfoTable );

        hitRatioGroup.setRefreshFunction( () -> {
            long hits = hitsCounter.longValue();
            long misses = missesCounter.longValue();
            double hitPercent = (double) hits / (hits + misses);
            double missesPercent = 1.0 - hitPercent;

            hitInfoGraph.updateGraph(
                    new String[]{ "Misses", "Hits" },
                    new GraphData<>( "heap-data", new Long[]{ misses, hits } )
            );

            DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance();
            symbols.setDecimalSeparator( '.' );
            DecimalFormat df = new DecimalFormat( "#.0", symbols );
            hitInfoTable.reset();
            hitInfoTable.addRow( "Hits", df.format( hitPercent * 100 ) + " %", hits );
            hitInfoTable.addRow( "Misses", df.format( missesPercent * 100 ) + " %", misses );
        } );

        // Invalidate cache
        InformationGroup invalidateGroup = new InformationGroup( page, "Invalidate" ).setOrder( 3 );
        im.addGroup( invalidateGroup );

        InformationText invalidateText = new InformationText( invalidateGroup, "Invalidate the query plan cache including the hit and miss counters." );
        invalidateText.setOrder( 1 );
        im.registerInformation( invalidateText );

        InformationAction invalidateAction = new InformationAction( invalidateGroup, "Invalidate", parameters -> {
            reset();
            return "Successfully invalidated the query plan cache!";
        } );
        invalidateAction.setOrder( 2 );
        im.registerInformation( invalidateAction );
    }
}
