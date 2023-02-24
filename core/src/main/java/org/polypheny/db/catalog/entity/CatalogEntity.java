/*
 * Copyright 2019-2023 The Polypheny Project
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
import org.apache.calcite.linq4j.Queryable;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.Catalog.EntityType;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.plan.AlgOptEntity.ToAlgContext;
import org.polypheny.db.schema.PolyphenyDbSchema;
import org.polypheny.db.schema.Wrapper;

public abstract class CatalogEntity implements Wrapper, Serializable {

    public final long id;
    public final EntityType entityType;
    public final NamespaceType namespaceType;
    public final String name;


    protected CatalogEntity( long id, String name, EntityType type, NamespaceType namespaceType ) {
        this.id = id;
        this.name = name;
        this.entityType = type;
        this.namespaceType = namespaceType;
    }


    public AlgDataType getRowType() {
        return null;
    }


    @Deprecated
    public boolean rolledUpColumnValidInsideAgg() {
        return true;
    }


    @Deprecated
    public boolean isRolledUp( String fieldName ) {
        return false;
    }


    public <E> Queryable<E> asQueryable( DataContext root, PolyphenyDbSchema schema, String tableName ) {
        throw new UnsupportedOperationException( "Not implemented by store" );
    }


    public AlgNode toAlg( ToAlgContext toAlgContext, CatalogGraphDatabase graph ) {
        throw new UnsupportedOperationException( "Not implemented by store" );
    }

}
