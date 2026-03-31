/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.adapter.parquet.schema;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.parquet.ParquetSource;
import org.polypheny.db.adapter.parquet.execution.ParquetDocumentEnumerator;
import org.polypheny.db.adapter.parquet.planning.ParquetDocumentScan;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.catalog.entity.physical.PhysicalCollection;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.schema.types.ScannableEntity;
import org.polypheny.db.schema.types.TranslatableEntity;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Source;

public class ParquetDocument extends PhysicalCollection implements ScannableEntity, TranslatableEntity {

    private final Source source;
    private final ParquetSource parquetSource;


    public ParquetDocument( PhysicalCollection collection, Source source, ParquetSource parquetSource ) {
        super(
                collection.id,
                collection.allocationId,
                collection.logicalId,
                collection.namespaceId,
                collection.name,
                collection.namespaceName,
                collection.adapterId );
        this.source = source;
        this.parquetSource = parquetSource;
    }


    @Override
    public Expression asExpression() {
        Expression argExp = Expressions.constant( this.id );
        return Expressions.convert_(
                Expressions.call(
                        Expressions.call( this.parquetSource.asExpression(), "getAdapterCatalog" ),
                        "getPhysical",
                        argExp ),
                ScannableEntity.class );
    }


    @Override
    public Enumerable<PolyValue[]> scan( DataContext dataContext ) {
        dataContext.getStatement().getTransaction().registerInvolvedAdapter( parquetSource );
        final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get( dataContext );
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<PolyValue[]> enumerator() {
                return new ParquetDocumentEnumerator( source, cancelFlag );
            }
        };
    }


    @Override
    public AlgNode toAlg( AlgCluster cluster, AlgTraitSet traitSet ) {
        return new ParquetDocumentScan( cluster, this );
    }

}
