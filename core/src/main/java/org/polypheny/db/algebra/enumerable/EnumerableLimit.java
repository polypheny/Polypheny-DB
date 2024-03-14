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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.algebra.enumerable;


import java.util.List;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgDistributionTraitDef;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.metadata.AlgMdCollation;
import org.polypheny.db.algebra.metadata.AlgMdDistribution;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.BuiltInMethod;


/**
 * Relational expression that applies a limit and/or offset to its input.
 */
public class EnumerableLimit extends SingleAlg implements EnumerableAlg {

    public final RexNode offset;
    public final RexNode fetch;


    /**
     * Creates an EnumerableLimit.
     * <p>
     * Use {@link #create} unless you know what you're doing.
     */
    public EnumerableLimit( AlgCluster cluster, AlgTraitSet traitSet, AlgNode input, RexNode offset, RexNode fetch ) {
        super( cluster, traitSet, input );
        this.offset = offset;
        this.fetch = fetch;
        assert getConvention() instanceof EnumerableConvention;
        assert getConvention() == input.getConvention();
    }


    /**
     * Creates an EnumerableLimit.
     */
    public static EnumerableLimit create( final AlgNode input, RexNode offset, RexNode fetch ) {
        final AlgCluster cluster = input.getCluster();
        final AlgMetadataQuery mq = cluster.getMetadataQuery();
        final AlgTraitSet traitSet =
                cluster.traitSetOf( EnumerableConvention.INSTANCE )
                        .replaceIfs( AlgCollationTraitDef.INSTANCE, () -> AlgMdCollation.limit( mq, input ) )
                        .replaceIf( AlgDistributionTraitDef.INSTANCE, () -> AlgMdDistribution.limit( mq, input ) );
        return new EnumerableLimit( cluster, traitSet, input, offset, fetch );
    }


    @Override
    public EnumerableLimit copy( AlgTraitSet traitSet, List<AlgNode> newInputs ) {
        return new EnumerableLimit( getCluster(), traitSet, sole( newInputs ), offset, fetch );
    }


    @Override
    public String algCompareString() {
        return this.getClass().getSimpleName() + "$" +
                input.algCompareString() + "$" +
                (offset != null ? offset.hashCode() + "$" : "") +
                (fetch != null ? fetch.hashCode() : "") + "&";
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw )
                .itemIf( "offset", offset, offset != null )
                .itemIf( "fetch", fetch, fetch != null );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        final BlockBuilder builder = new BlockBuilder();
        final EnumerableAlg child = (EnumerableAlg) getInput();
        final Result result = implementor.visitChild( this, 0, child, pref );
        final PhysType physType = PhysTypeImpl.of( implementor.getTypeFactory(), getTupleType(), result.format() );

        Expression v = builder.append( "child", result.block() );
        if ( offset != null ) {
            v = builder.append(
                    "offset",
                    Expressions.call( v, BuiltInMethod.SKIP.method, getExpression( offset ) ) );
        }
        if ( fetch != null ) {
            v = builder.append(
                    "fetch",
                    Expressions.call( v, BuiltInMethod.TAKE.method, getExpression( fetch ) ) );
        }

        builder.add( Expressions.return_( null, v ) );
        return implementor.result( physType, builder.toBlock() );
    }


    private static Expression getExpression( RexNode offset ) {
        if ( offset instanceof RexDynamicParam param ) {
            return Expressions.convert_(
                    Expressions.call( DataContext.ROOT, BuiltInMethod.DATA_CONTEXT_GET_PARAMETER_VALUE.method, Expressions.constant( param.getIndex() ) ),
                    Integer.class );
        } else {
            return Expressions.constant( RexLiteral.intValue( offset ) );
        }
    }

}

