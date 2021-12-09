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

package org.polypheny.db.processing.shuttles;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.routing.LogicalQueryInformation;


public class LogicalQueryInformationImpl implements LogicalQueryInformation {

    @Getter
    protected final Map<Long, String> availableColumns; // column id -> schemaName.tableName.ColumnName
    protected final Map<Long, Long> availableColumnsWithTable; // columnId -> tableId
    protected final Map<Integer, List<Long>> accessedPartitions; // scanId  -> partitionIds
    protected final String queryId;
    protected final Map<Long, String> usedColumns;

    @Getter
    protected final List<String> tables;


    public LogicalQueryInformationImpl(
            String queryId,
            Map<Integer, List<Long>> accessedPartitionMap, // scanId -> List of partitionIds
            LinkedHashMap<Long, String> availableColumns,
            HashMap<Long, Long> availableColumnsWithTable,
            Map<Long, String> usedColumns,
            List<String> tables ) {
        this.queryId = queryId;
        this.accessedPartitions = accessedPartitionMap;
        this.availableColumns = availableColumns;
        this.availableColumnsWithTable = availableColumnsWithTable;
        this.usedColumns = usedColumns;
        this.tables = tables;
    }


    @Override
    public Map<Integer, List<Long>> getAccessedPartitions() {
        return this.accessedPartitions;
    }


    @Override
    public Map<Long, Long> getAvailableColumnsWithTable() {
        return this.availableColumnsWithTable;
    }


    @Override
    public List<Long> getAllColumnsPerTable( Long tableId ) {
        final Map<Long, String> usedCols = this.availableColumns;
        return availableColumnsWithTable.entrySet().stream()
                .filter( x -> x.getValue().equals( tableId ) && usedCols.keySet().contains( x.getKey() ) )
                .map( Entry::getKey )
                .collect( Collectors.toList() );
    }


    @Override
    public List<Long> getUsedColumnsPerTable( Long tableId ) {
        Map<Long, String> usedCols = getUsedColumns();
        return availableColumnsWithTable.entrySet().stream()
                .filter( x -> x.getValue().equals( tableId ) && usedCols.keySet().contains( x.getKey() ) )
                .map( Entry::getKey )
                .collect( Collectors.toList() );
    }


    @Override
    public Map<Long, String> getUsedColumns() {
        return this.usedColumns;
    }


    @Override
    public String getQueryClass() {
        return this.queryId;
    }

}
