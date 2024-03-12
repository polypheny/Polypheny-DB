/*
 * Copyright 2019-2024 The Polypheny Project
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

package org.polypheny.db.processing.shuttles;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import lombok.Value;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.routing.LogicalQueryInformation;

@Value
public class LogicalQueryInformationImpl implements LogicalQueryInformation {

    public String queryHash;
    public ImmutableMap<Long, List<Long>> accessedPartitions; // scanId  -> partitionIds
    public ImmutableMap<Long, String> availableColumns; // column id -> schemaName.tableName.ColumnName
    public ImmutableMap<Long, Long> availableColumnsWithTable; // columnId -> tableId
    public ImmutableMap<Long, String> usedColumns;
    public ImmutableMap<DataModel, Set<Long>> scannedEntities;
    public ImmutableMap<DataModel, Set<Long>> modifiedEntities;
    public ImmutableSet<Long> allModifiedEntities;
    public ImmutableSet<Long> allScannedEntities;
    public ImmutableSet<Long> allEntities;


    public LogicalQueryInformationImpl(
            String queryHash,
            Map<Long, List<Long>> accessedPartitions, // scanId -> List of partitionIds
            Map<Long, String> availableColumns,
            Map<Long, Long> availableColumnsWithTable,
            Map<Long, String> usedColumns,
            Map<DataModel, Set<Long>> scannedEntities,
            Map<DataModel, Set<Long>> modifiedEntities ) {
        this.queryHash = queryHash;
        this.accessedPartitions = ImmutableMap.copyOf( accessedPartitions );
        this.availableColumns = ImmutableMap.copyOf( availableColumns );
        this.availableColumnsWithTable = ImmutableMap.copyOf( availableColumnsWithTable );
        this.usedColumns = ImmutableMap.copyOf( usedColumns );
        this.scannedEntities = ImmutableMap.copyOf( scannedEntities );
        this.modifiedEntities = ImmutableMap.copyOf( modifiedEntities );
        this.allModifiedEntities = buildAllModifiedEntities();
        this.allScannedEntities = buildAllScannedEntities();
        this.allEntities = buildAllEntities();
    }


    private ImmutableSet<Long> buildAllEntities() {
        Builder<Long> set = ImmutableSet.builder();
        allModifiedEntities.forEach( set::add );
        allScannedEntities.forEach( set::add );
        return set.build();
    }


    private ImmutableSet<Long> buildAllScannedEntities() {
        return ImmutableSet.copyOf( scannedEntities.values().stream().flatMap( Collection::stream ).toList() );
    }


    private ImmutableSet<Long> buildAllModifiedEntities() {
        return ImmutableSet.copyOf( modifiedEntities.values().stream().flatMap( Collection::stream ).toList() );
    }


    @Override
    public List<Long> getAllColumnsPerTable( Long tableId ) {
        final Map<Long, String> usedCols = this.availableColumns;
        return availableColumnsWithTable.entrySet().stream()
                .filter( x -> x.getValue().equals( tableId ) && usedCols.containsKey( x.getKey() ) )
                .map( Entry::getKey )
                .toList();
    }


    @Override
    public List<Long> getUsedColumnsPerTable( Long tableId ) {
        Map<Long, String> usedCols = getUsedColumns();
        return availableColumnsWithTable.entrySet().stream()
                .filter( x -> x.getValue().equals( tableId ) && usedCols.containsKey( x.getKey() ) )
                .map( Entry::getKey )
                .toList();
    }

}
