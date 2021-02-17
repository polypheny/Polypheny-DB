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

package org.polypheny.db.catalog.entity;

import java.io.Serializable;
import lombok.EqualsAndHashCode;
import lombok.SneakyThrows;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.TableType;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;


@EqualsAndHashCode
public final class CatalogView implements CatalogEntity, Comparable<CatalogView>{

    public final long id;
    public final String name;
    final long schemaId;
    public final long databaseId;
    public final int ownerId;
    public final String ownerName;
    public final boolean modifiable;
    public final String definition;
    public final RelNode relRoot;



    public CatalogView( long id, String name, long schemaId, long databaseId, int ownerId, String ownerName, boolean modifiable, String definition, RelNode relRoot ) {
        this.id = id;
        this.name = name;
        this.schemaId = schemaId;
        this.databaseId = databaseId;
        this.ownerId = ownerId;
        this.ownerName = ownerName;
        this.modifiable = modifiable;
        this.definition = definition;
        this.relRoot = relRoot;
    }

    @SneakyThrows
    public String getDatabaseName() {
        return Catalog.getInstance().getDatabase( databaseId ).name;
    }


    @SneakyThrows
    public String getSchemaName() {

        return Catalog.getInstance().getSchema( schemaId ).name;
    }


    @Override
    public int compareTo( CatalogView o ) {
        if ( o != null ) {
            int comp = (int) (this.databaseId - o.databaseId);
            if ( comp == 0 ) {
                comp = (int) (this.schemaId - o.schemaId);
                if ( comp == 0 ) {
                    return (int) (this.id - o.id);
                } else {
                    return comp;
                }

            } else {
                return comp;
            }
        }
        return -1;
    }

    /*
    TODO IG: for UI
     */
    @Override
    public Serializable[] getParameterArray(){
       // return new Serializable[]{};
        return new Serializable[]{
                getDatabaseName(),
                getSchemaName(),
                name,
                TableType.TABLE,
                "",
                null,
                null,
                null,
                null,
                null,
                ownerName,
                definition };
    }



}
