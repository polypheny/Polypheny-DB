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

package org.polypheny.db.partition;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;

@Slf4j
public class RoundRobinPartitionManager extends AbstractPartitionManager {

    boolean hasUnboundPartition = false;


    @Override
    public long getTargetPartitionId( CatalogTable catalogTable, String columnValue ) {
        log.debug( "RoundRobinPartitionManager getPartitionId()" );

        return -1;
    }


    @Override
    public boolean validatePartitionDistribution( CatalogTable table ) {
        log.debug( "RoundRobinPartitionManager validPartitionDistribution()" );
        return false;
    }


    // Needed when columnPlacements are being dropped
    @Override
    public boolean probePartitionDistributionChange( CatalogTable catalogTable, int storeId, long columnId ) {
        return false;

    }


    @Override
    public List<CatalogColumnPlacement> getRelevantPlacements( CatalogTable catalogTable, List<Long> partitionIds ) {
        return null;
    }


    @Override
    public boolean validatePartitionSetup( List<String> partitionQualifiers, long numPartitions, List<String> partitionNames ) {
        super.validatePartitionSetup( partitionQualifiers, numPartitions, partitionNames );
        return false;
    }


    @Override
    public boolean allowsUnboundPartition() {
        return false;
    }

}
