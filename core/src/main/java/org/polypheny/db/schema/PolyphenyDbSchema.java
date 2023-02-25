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

package org.polypheny.db.schema;

import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogNamespace;
import org.polypheny.db.catalog.entity.LogicalCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationCollection;
import org.polypheny.db.catalog.entity.allocation.AllocationGraph;
import org.polypheny.db.catalog.entity.allocation.AllocationTable;
import org.polypheny.db.catalog.entity.logical.LogicalGraph;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.entity.physical.PhysicalCollection;
import org.polypheny.db.catalog.entity.physical.PhysicalGraph;
import org.polypheny.db.catalog.entity.physical.PhysicalTable;
import org.polypheny.db.catalog.logistic.Pattern;


public interface PolyphenyDbSchema {


    default LogicalTable getTable( List<String> names ) {
        switch ( names.size() ) {
            case 3:
                return Catalog.getInstance().getTables( Pattern.of( names.get( 0 ) ), Pattern.of( names.get( 1 ) ), Pattern.of( names.get( 2 ) ) ).get( 0 );
            case 2:
                return Catalog.getInstance().getTables( Catalog.defaultDatabaseId, Pattern.of( names.get( 0 ) ), Pattern.of( names.get( 1 ) ) ).get( 0 );
            case 1:
                return Catalog.getInstance().getTables( Catalog.defaultDatabaseId, null, Pattern.of( names.get( 0 ) ) ).get( 0 );
            default:
                return null;
        }
    }

    default LogicalTable getTable( long id ) {
        return Catalog.getInstance().getTable( id );
    }

    default AllocationTable getAllocTable( long id ){
        return null;
    }

    default PhysicalTable getPhysicalTable( long id ){
        return null;
    }


    default LogicalCollection getCollection( List<String> names ) {
        CatalogNamespace namespace;
        switch ( names.size() ) {
            case 3:
                namespace = Catalog.getInstance().getSchemas( Pattern.of( names.get( 0 ) ), Pattern.of( names.get( 1 ) ) ).get( 0 );
                return Catalog.getInstance().getCollections( namespace.id, Pattern.of( names.get( 2 ) ) ).get( 0 );
            case 2:
                namespace = Catalog.getInstance().getSchemas( Catalog.defaultDatabaseId, Pattern.of( names.get( 0 ) ) ).get( 0 );
                return Catalog.getInstance().getCollections( namespace.id, Pattern.of( names.get( 1 ) ) ).get( 0 );
            case 1:
                // TODO add methods
                namespace = Catalog.getInstance().getSchemas( Catalog.defaultDatabaseId, null ).get( 0 );
                return Catalog.getInstance().getCollections( namespace.id, Pattern.of( names.get( 0 ) ) ).get( 0 );
            default:
                return null;
        }
    }

    default LogicalCollection getCollection( long id ) {
        return Catalog.getInstance().getCollection( id );
    }

    default AllocationCollection getAllocCollection( long id ){
        return null;
    }

    default PhysicalCollection getPhysicalCollection( long id ){
        return null;
    }

    default LogicalGraph getGraph( List<String> names ) {
        if ( names.size() == 1 ) {// TODO add methods
            return Catalog.getInstance().getGraphs( Catalog.defaultDatabaseId, Pattern.of( names.get( 0 ) ) ).get( 0 );
        }
        return null;
    }

    default LogicalGraph getGraph( long id ) {
        return Catalog.getInstance().getGraph( id );
    }

    default AllocationGraph getAllocGraph( long id ){
        return null;
    }

    default PhysicalGraph getPhysicalGraph( long id ){
        return null;
    }

    default List<String> getNamespaceNames() {
        return Catalog.getInstance().getSchemas( Catalog.defaultDatabaseId, null ).stream().map( t -> t.name ).collect( Collectors.toList() );
    }

    default boolean isPartitioned( long id ){
        return false;
    }

}
