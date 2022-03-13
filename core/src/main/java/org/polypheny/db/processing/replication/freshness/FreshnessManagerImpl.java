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

package org.polypheny.db.processing.replication.freshness;


import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.processing.replication.freshness.exceptions.UnknownFreshnessEvaluationTypeRuntimeException;
import org.polypheny.db.processing.replication.freshness.properties.FreshnessSpecification;


public class FreshnessManagerImpl extends FreshnessManager {

    Catalog catalog = Catalog.getInstance();


    @Override
    public double transformToFreshnessIndex( CatalogTable table, String s, EvaluationType evaluationType ) {
        return 0;
    }


    @Override
    public List<CatalogPartitionPlacement> getRelevantPartitionPlacements( CatalogTable table, List<Long> partitionIds, FreshnessSpecification specs ) {

        List<CatalogPartitionPlacement> proposedPlacements = new ArrayList<>();

        switch ( specs.getEvaluationType() ) {

            case TIMESTAMP:
            case DELAY:

                proposedPlacements = handleTimestampFreshness( table, specs.getToleratedTimestamp() );
                break;

            case PERCENTAGE:
            case INDEX:
                proposedPlacements = handleFreshnessIndex( table, specs.getFreshnessIndex() );
                break;

            default:
                throw new UnknownFreshnessEvaluationTypeRuntimeException( specs.getEvaluationType().toString() );
        }

        return proposedPlacements;
    }


    private List<CatalogPartitionPlacement> handleTimestampFreshness( CatalogTable table, Timestamp toleratedTimestamp ) {

        return null;
    }


    private List<CatalogPartitionPlacement> handleFreshnessIndex( CatalogTable table, double freshnessIndex ) {

        return null;
    }
}
