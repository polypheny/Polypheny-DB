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

package org.polypheny.db.sql.sql.util.replication.properties;


import java.util.Map;
import java.util.Map.Entry;
import org.polypheny.db.catalog.Catalog.DataPlacementRole;
import org.polypheny.db.catalog.Catalog.ReplicationStrategy;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.UnknownPlacementRoleException;
import org.polypheny.db.catalog.exceptions.UnknownReplicationStrategyException;
import org.polypheny.db.nodes.Identifier;
import org.polypheny.db.nodes.Literal;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.replication.properties.PlacementPropertyExtractor;
import org.polypheny.db.replication.properties.PlacementPropertyInformation;
import org.polypheny.db.replication.properties.exception.UnknownPlacementPropertyException;
import org.polypheny.db.sql.sql.SqlIdentifier;


public class SqlPlacementPropertyExtractor extends PlacementPropertyExtractor {

    protected SqlPlacementPropertyExtractor( Map<Identifier, Identifier> propertyMap ) {
        super( propertyMap );
    }


    public static PlacementPropertyInformation fromNodeLists( CatalogTable table, Map<SqlIdentifier, SqlIdentifier> propertyMapping ) throws UnknownPlacementRoleException, UnknownPlacementPropertyException, UnknownReplicationStrategyException {

        if ( propertyMapping == null || propertyMapping.isEmpty() ) {
            return null;
        }

            /*CatalogDataPlacement dataPlacement = Catalog.getInstance().getDataPlacement( storeInstance.getAdapterId(), placementPropertyInfo.table.id ) ;
            if ( dataPlacement.dataPlacementRole.equals( placementPropertyInfo. ) )

            }*/

        for ( Entry<SqlIdentifier, SqlIdentifier> entry : propertyMapping.entrySet() ) {

            switch ( entry.getKey().getSimple().toUpperCase() ) {
                case "ROLE":
                    dataPlacementRole = DataPlacementRole.getByName( entry.getValue().getSimple().toUpperCase() );
                    break;

                case "REPLICATION_STRATEGY":
                    // TODO @HENNLO IF = EAGER and perfore was LAZY make sure that it is first becomes uptodate,
                    //  either by applying all pending changes or via DataMigrator full copy

                    replicationStrategy = ReplicationStrategy.getByName( entry.getValue().getSimple().toUpperCase() );
                    break;

                default:
                    throw new UnknownPlacementPropertyException( entry.getKey().getSimple() );
            }

        }

        return createPlacementPropertyInformation( table );
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
