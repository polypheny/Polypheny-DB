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
import org.polypheny.db.catalog.Catalog.PlacementState;
import org.polypheny.db.catalog.Catalog.ReplicationStrategy;
import org.polypheny.db.catalog.entity.CatalogEntity;
import org.polypheny.db.catalog.exceptions.UnknownPlacementStateException;
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


    public static PlacementPropertyInformation fromNodeLists( CatalogEntity entity, Map<SqlIdentifier, SqlIdentifier> propertyMapping ) throws UnknownPlacementStateException, UnknownPlacementPropertyException, UnknownReplicationStrategyException {

        if ( propertyMapping == null || propertyMapping.isEmpty() ) {
            return null;
        }

        for ( Entry<SqlIdentifier, SqlIdentifier> entry : propertyMapping.entrySet() ) {

            switch ( entry.getKey().getSimple().toUpperCase() ) {
                case "STATE":
                    placementState = PlacementState.getByName(
                            entry.getValue().getSimple().toUpperCase().equals( "OUTDATED" )
                                    ? "INFINITELY_OUTDATED" : entry.getValue().getSimple().toUpperCase()
                    );
                    break;

                case "REPLICATION_STRATEGY":
                    replicationStrategy = ReplicationStrategy.getByName( entry.getValue().getSimple().toUpperCase() );
                    break;

                default:
                    throw new UnknownPlacementPropertyException( entry.getKey().getSimple() );
            }

        }

        return createPlacementPropertyInformation( entity );
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
