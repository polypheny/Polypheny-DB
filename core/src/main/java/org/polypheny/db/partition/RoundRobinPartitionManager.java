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

import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogColumnPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.routing.Router;

import java.util.List;

public class RoundRobinPartitionManager extends AbstractPartitionManager{


    @Override
    public long getTargetPartitionId(CatalogTable catalogTable, String columnValue) {
        System.out.println("HENNLO  RoundRobinPartitionManager getPartitionId()");
        //IDEA: mySchema_testTable_sales_RANGE_100

        return -1;
    }

    @Override
    public boolean validatePartitionDistribution(CatalogTable table) {
        System.out.println("HENNLO  RoundRobinPartitionManager validPartitionDistribution()");
        return false;
    }

    //Needed when columnPlacements are being dropped
    @Override
    public boolean probePartitionDistributionChange(CatalogTable catalogTable, int storeId, long columnId){
    //TODO nOt implemented yet
        return false;

    }

    @Override
    public List<CatalogColumnPlacement> getRelevantPlacements(CatalogTable catalogTable, long partitionId) {
        return null;
    }




}
