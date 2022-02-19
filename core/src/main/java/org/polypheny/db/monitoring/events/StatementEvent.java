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

package org.polypheny.db.monitoring.events;


import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.PolyResult;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.routing.LogicalQueryInformation;
import org.polypheny.db.transaction.Statement;


/**
 * Basis class needed for every statement type like, QUERY, DML, DDL
 */
@Setter
@Getter
public abstract class StatementEvent extends BaseEvent {

    protected String monitoringType;
    protected AlgRoot routed;
    protected PolyResult result;
    protected Statement statement;
    protected List<List<Object>> rows;
    protected String description;
    protected List<String> fieldNames;
    protected long executionTime;
    protected int rowCount;
    protected boolean isAnalyze;
    protected boolean isSubQuery;
    protected boolean isCommitted;
    protected String durations;
    protected Map<Long, Set<Long>> accessedPartitions;
    protected LogicalQueryInformation logicalQueryInformation;
    protected String algCompareString;
    protected String physicalQueryClass;
    protected final HashMap<Long, List<Object>> changedValues = new HashMap<>();
    protected Integer indexSize = null;
    // Only used for ddl events
    protected long tableId;
    // Only used for ddl events
    protected long schemaId;
    // Only used for ddl events
    protected long columnId;


    @Override
    public abstract <T extends MonitoringDataPoint> List<Class<T>> getMetrics();


    @Override
    public <T extends MonitoringDataPoint> List<Class<T>> getOptionalMetrics() {
        return Collections.emptyList();
    }


    @Override
    public abstract List<MonitoringDataPoint> analyze();


    /**
     * Is used to update the partitions which have been used within a transaction.
     * This method merges existing accesses with new ones
     *
     * @param updatedPartitionsList partitions per table that have been accessed and should be added to the overall statistics
     */
    public void updateAccessedPartitions( Map<Long, Set<Long>> updatedPartitionsList ) {
        Long tableId;
        Set<Long> partitionIds;

        for ( Entry<Long, Set<Long>> entry : updatedPartitionsList.entrySet() ) {
            tableId = entry.getKey();
            partitionIds = entry.getValue();

            // Initialize if this is the first time accessing
            if ( accessedPartitions == null ) {
                accessedPartitions = new HashMap<>();
            }

            if ( !accessedPartitions.containsKey( tableId ) ) {
                accessedPartitions.put( tableId, new HashSet<>() );
            }

            Set<Long> mergedPartitionIds = Stream.of( accessedPartitions.get( tableId ), partitionIds )
                    .flatMap( Set::stream )
                    .collect( Collectors.toSet() );

            accessedPartitions.replace( tableId, mergedPartitionIds );
        }
    }

}
