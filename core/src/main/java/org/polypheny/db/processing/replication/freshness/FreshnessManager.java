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


import java.util.List;
import org.polypheny.db.catalog.entity.CatalogPartitionPlacement;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.nodes.Literal;
import org.polypheny.db.nodes.Node;


/**
 * This class is used to transform and extract relevant information out of a query which specified the optional acceptance to retrieve outdated data.
 */
public abstract class FreshnessManager {


    public abstract double transformToFreshnessIndex( CatalogTable table, String s, FreshnessInformation.evaluationType evaluationType );

    public abstract List<CatalogPartitionPlacement> getRelevantPartitionPlacements( CatalogTable table, double freshnessIndex );


    public static class FreshnessInformation {

        public enum evaluationType {
            TIMESTAMP,
            DELAY,
            PERCENTAGE
        }


        public FreshnessInformation() {

        }


        public static FreshnessInformation fromNodeLists() {

            return null;
        }


        /**
         * Needed to modify strings otherwise the SQL-input 'a' will be also added as the value "'a'" and not as "a" as intended
         * Essentially removes " ' " at the start and end of value
         *
         * @param node Node to be modified
         * @return String
         */
        public static String getValueOfSqlNode( Node node ) {

            if ( node instanceof Literal ) {
                return ((Literal) node).toValue();
            }
            return node.toString();
        }

    }

}
