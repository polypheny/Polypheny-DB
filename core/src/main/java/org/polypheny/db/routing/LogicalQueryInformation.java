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

package org.polypheny.db.routing;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.polypheny.db.catalog.logistic.DataModel;


/**
 * This is the result of the analyze step in the query pipeline.
 * The class contains logical information about a query.
 */

public interface LogicalQueryInformation {

    /**
     * @return A map with tableId as key and a list of partitionIds as value.
     */
    Map<Long, List<Long>> getAccessedPartitions(); // tableId  -> partitionIds

    /**
     * @return A map with columnId as key and tableId as value.
     */
    Map<Long, Long> getAvailableColumnsWithTable(); // columnId -> tableId

    /**
     * @return A map with columnIds as key and column full names as values.
     */
    Map<Long, String> getUsedColumns(); // columnId -> schemaName.tableName.columnName

    /**
     * @param tableId The id of the table
     * @return a list of all columnIds.
     */
    List<Long> getAllColumnsPerTable( Long tableId );

    /**
     * @param tableId The id of the table
     * @return a list of all used columnIds in the query.
     */
    List<Long> getUsedColumnsPerTable( Long tableId );

    /**
     * @return gets the query class.
     */
    String getQueryHash();

    ImmutableMap<DataModel, Set<Long>> getScannedEntities();

    ImmutableMap<DataModel, Set<Long>> getModifiedEntities();

    ImmutableSet<Long> getAllModifiedEntities();

    ImmutableSet<Long> getAllScannedEntities();

    ImmutableSet<Long> getAllEntities();

}
