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


import com.google.common.collect.ImmutableList;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Primitive;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgDistributionTraitDef;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.metadata.AlgMdCollation;
import org.polypheny.db.algebra.metadata.AlgMdDistribution;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.prepare.JavaTypeFactoryImpl;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Pair;


/**
 * Implementation of {@link org.polypheny.db.algebra.core.Values} in {@link EnumerableConvention enumerable calling convention}.
 */
public class EnumerableValues extends Values implements EnumerableAlg {

    /**
     * Creates an EnumerableValues.
     */
    private EnumerableValues( AlgCluster cluster, AlgDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples, AlgTraitSet traitSet ) {
        super( cluster, rowType, tuples, traitSet );
    }


    /**
     * Creates an EnumerableValues.
     */
    public static EnumerableValues create( AlgCluster cluster, final AlgDataType rowType, final ImmutableList<ImmutableList<RexLiteral>> tuples ) {
        final AlgMetadataQuery mq = cluster.getMetadataQuery();
        final AlgTraitSet traitSet =
                cluster.traitSetOf( EnumerableConvention.INSTANCE )
                        .replaceIfs( AlgCollationTraitDef.INSTANCE, () -> AlgMdCollation.values( mq, rowType, tuples ) )
                        .replaceIf( AlgDistributionTraitDef.INSTANCE, () -> AlgMdDistribution.values( rowType, tuples ) );
        return new EnumerableValues( cluster, rowType, tuples, traitSet );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        assert inputs.isEmpty();
        return create( getCluster(), rowType, tuples );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        /*
          return Linq4j.asEnumerable(
              new Object[][] {
                  new Object[] {1, 2},
                  new Object[] {3, 4}
              });
        */
        final JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();
        final BlockBuilder builder = new BlockBuilder();
        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getTupleType(),
                        pref.preferCustom() );
        final Type rowClass = physType.getJavaTupleType();

        final List<Expression> expressions = new ArrayList<>();
        final List<AlgDataTypeField> fields = rowType.getFields();
        for ( List<RexLiteral> tuple : tuples ) {
            final List<Expression> literals = new ArrayList<>();
            for ( Pair<AlgDataTypeField, RexLiteral> pair : Pair.zip( fields, tuple ) ) {
                literals.add(
                        RexToLixTranslator.translateLiteral(
                                pair.right,
                                pair.left.getType(),
                                typeFactory,
                                RexImpTable.NullAs.NULL ) );
            }
            expressions.add( physType.record( literals ) );
        }
        builder.add(
                Expressions.return_(
                        null,
                        Expressions.call(
                                BuiltInMethod.AS_ENUMERABLE.method,
                                Expressions.newArrayInit( Primitive.box( rowClass ), 2, expressions ) ) ) );
        return implementor.result( physType, builder.toBlock() );
    }

}

