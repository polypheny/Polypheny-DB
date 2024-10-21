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

package org.polypheny.db.adapter.json;

import java.net.URL;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.catalogs.DocAdapterCatalog;
import org.polypheny.db.catalog.entity.physical.PhysicalCollection;
import org.polypheny.db.catalog.entity.physical.PhysicalEntity;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.schema.types.ScannableEntity;
import org.polypheny.db.schema.types.TranslatableEntity;
import org.polypheny.db.type.entity.PolyValue;

final class JsonCollection extends PhysicalCollection implements ScannableEntity, TranslatableEntity {

    private final URL url;
    private final Adapter<DocAdapterCatalog> adapter;


    JsonCollection( URL url, PhysicalEntity collection, long allocationId, JsonNamespace namespace, Adapter<DocAdapterCatalog> adapter ) {
        super( collection.getId(), allocationId, collection.getLogicalId(), namespace.getId(), collection.getName(), namespace.getName(), adapter.getAdapterId() );
        this.url = url;
        this.adapter = adapter;
    }


    @Override
    public Expression asExpression() {
        Expression argExp = Expressions.constant( this.id );
        return Expressions.convert_( Expressions.call( Expressions.call( this.adapter.asExpression(), "getAdapterCatalog" ), "getPhysical", argExp ), JsonCollection.class );
    }


    @Override
    public Enumerable<PolyValue[]> scan( DataContext dataContext ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( adapter );
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<PolyValue[]> enumerator() {
                return new JsonEnumerator( url );
            }
        };
    }


    public AlgNode toAlg( AlgCluster cluster, AlgTraitSet traitSet ) {
        return new JsonScan( cluster, this );
    }


    public Enumerable<PolyValue[]> project( final DataContext dataContext ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( adapter );
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<PolyValue[]> enumerator() {
                return new JsonEnumerator( url );
            }
        };
    }

}

