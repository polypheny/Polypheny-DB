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

package org.polypheny.db.adapter.xml;

import java.net.URL;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.Adapter;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.DataContext.Variable;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.catalogs.DocAdapterCatalog;
import org.polypheny.db.catalog.entity.physical.PhysicalCollection;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.schema.types.ScannableEntity;
import org.polypheny.db.schema.types.TranslatableEntity;
import org.polypheny.db.type.entity.PolyValue;

public class XmlCollection extends PhysicalCollection implements ScannableEntity, TranslatableEntity {

    private final URL url;
    private final Adapter<DocAdapterCatalog> adapter;


    private XmlCollection( Builder builder ) {
        super( builder.collectionId, builder.allocationId, builder.logicalId, builder.namespaceId, builder.collectionName, builder.namespaceName, builder.adapter.getAdapterId() );
        this.url = builder.url;
        this.adapter = builder.adapter;
    }


    @Override
    public Expression asExpression() {
        Expression argExp = Expressions.constant( this.id );
        return Expressions.convert_( Expressions.call( Expressions.call( this.adapter.asExpression(), "getAdapterCatalog" ), "getPhysical", argExp ), XmlCollection.class );
    }


    @Override
    public Enumerable<PolyValue[]> scan( DataContext dataContext ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( adapter );
        final AtomicBoolean cancelFlag = Variable.CANCEL_FLAG.get( dataContext );
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<PolyValue[]> enumerator() {
                return new XmlEnumerator( url );
            }
        };
    }


    public AlgNode toAlg( AlgCluster cluster, AlgTraitSet traitSet ) {
        return new XmlScan( cluster, this, new int[]{ 0 } );
    }


    public Enumerable<PolyValue[]> project( final DataContext dataContext, final int[] fields ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( adapter );
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<PolyValue[]> enumerator() {
                return new XmlEnumerator( url );
            }
        };
    }


    public static class Builder {

        private URL url;
        private long collectionId;
        private long allocationId;
        private long logicalId;
        private long namespaceId;
        private String collectionName;
        private String namespaceName;
        private Adapter<DocAdapterCatalog> adapter;


        public Builder url( URL uri ) {
            this.url = uri;
            return this;
        }


        public Builder collectionId( long id ) {
            this.collectionId = id;
            return this;
        }


        public Builder allocationId( long allocationId ) {
            this.allocationId = allocationId;
            return this;
        }


        public Builder logicalId( long logicalId ) {
            this.logicalId = logicalId;
            return this;
        }


        public Builder namespaceId( long namespaceId ) {
            this.namespaceId = namespaceId;
            return this;
        }


        public Builder collectionName( String name ) {
            this.collectionName = name;
            return this;
        }


        public Builder namespaceName( String namespaceName ) {
            this.namespaceName = namespaceName;
            return this;
        }


        public Builder adapter( Adapter<DocAdapterCatalog> adapter ) {
            this.adapter = adapter;
            return this;
        }


        public XmlCollection build() {
            return new XmlCollection( this );
        }

    }

}
