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

package org.polypheny.db.processing.caching;

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
import org.polypheny.db.prepare.Prepare.PreparedResult;
import org.polypheny.db.rel.RelNode;

public class ImplementationCache {

    public static final ImplementationCache INSTANCE = new ImplementationCache();

    private final Cache<String, PreparedResult> implementationCache;

    private final AtomicLong hitsCounter = new AtomicLong(); // Number of requests for which the cache contained the value
    private final AtomicLong missesCounter = new AtomicLong(); // Number of requests for which the cache hasn't contained the value
    private final AtomicLong uncacheableCounter = new AtomicLong(); // Number of requests for which the cache hasn't contained the value


    public ImplementationCache() {
        implementationCache = CacheBuilder.newBuilder()
                .maximumSize( RuntimeConfig.IMPLEMENTATION_CACHING_SIZE.getInteger() )
                //  .expireAfterWrite(10, TimeUnit.MINUTES)
                .build();
        registerMonitoringPage();
    }


    public PreparedResult getIfPresent( RelNode parameterizedNode ) {
        PreparedResult preparedResult = implementationCache.getIfPresent( parameterizedNode.relCompareString() );
        if ( preparedResult == null ) {
            missesCounter.incrementAndGet();
        } else {
            hitsCounter.incrementAndGet();
        }
        return preparedResult;
    }


    public void put( RelNode parameterizedNode, PreparedResult preparedResult ) {
        implementationCache.put( parameterizedNode.relCompareString(), preparedResult );
    }


    public void countUncacheable() {
        uncacheableCounter.incrementAndGet();
    }


    public void reset() {
        implementationCache.invalidateAll();
        hitsCounter.set( 0 );
        missesCounter.set( 0 );
        uncacheableCounter.set( 0 );
    }


    private void registerMonitoringPage() {
        InformationManager im = InformationManager.getInstance();

        InformationPage page = new InformationPage( "Implementation Cache" );
        im.addPage( page );

        // General
        InformationGroup generalGroup = new InformationGroup( page, "General" ).setOrder( 1 );
        im.addGroup( generalGroup );

        InformationKeyValue generalKv = new InformationKeyValue( generalGroup );
        im.registerInformation( generalKv );
        generalGroup.setRefreshFunction( () -> {
            generalKv.putPair( "Status", RuntimeConfig.IMPLEMENTATION_CACHING.getBoolean() ? "Active" : "Disabled" );
            generalKv.putPair( "Current Cache Size", implementationCache.size() + "" );
            generalKv.putPair( "Maximum Cache Size", RuntimeConfig.IMPLEMENTATION_CACHING_SIZE.getInteger() + "" );
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
            long misses = missesCounter.longValue() - uncacheableCounter.longValue();
            long uncacheable = uncacheableCounter.longValue();
            long total = hits + misses + uncacheable;

            double hitPercent = (double) hits / total;
            double missesPercent = (double) misses / total;
            double uncacheablePercent = 1.0 - hitPercent - missesPercent;

            hitInfoGraph.updateGraph(
                    new String[]{ "Misses", "Hits", "Uncacheable" },
                    new GraphData<>( "heap-data", new Long[]{ misses, hits, uncacheable } )
            );

            DecimalFormatSymbols symbols = DecimalFormatSymbols.getInstance();
            symbols.setDecimalSeparator( '.' );
            DecimalFormat df = new DecimalFormat( "0.0", symbols );
            hitInfoTable.reset();
            hitInfoTable.addRow( "Hits", df.format( total == 0 ? 0 : (hitPercent * 100) ) + " %", hits );
            hitInfoTable.addRow( "Misses", df.format( total == 0 ? 0 : (missesPercent * 100) ) + " %", misses );
            hitInfoTable.addRow( "Uncacheable", df.format( total == 0 ? 0 : (uncacheablePercent * 100) ) + " %", uncacheable );
        } );

        // Invalidate cache
        InformationGroup invalidateGroup = new InformationGroup( page, "Invalidate" ).setOrder( 3 );
        im.addGroup( invalidateGroup );

        InformationText invalidateText = new InformationText( invalidateGroup, "Invalidate the implementation cache including the hit and miss counters." );
        invalidateText.setOrder( 1 );
        im.registerInformation( invalidateText );

        InformationAction invalidateAction = new InformationAction( invalidateGroup, "Invalidate", parameters -> {
            reset();
            generalGroup.refresh();
            hitRatioGroup.refresh();
            return "Successfully invalidated the implementation cache!";
        } );
        invalidateAction.setOrder( 2 );
        im.registerInformation( invalidateAction );
    }

}
