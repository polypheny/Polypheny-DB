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

package org.polypheny.db.adapter.enumerable.document;

import com.google.common.collect.ImmutableList;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Primitive;
import org.bson.BsonValue;
import org.polypheny.db.adapter.enumerable.EnumerableAlg;
import org.polypheny.db.adapter.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.adapter.enumerable.EnumerableConvention;
import org.polypheny.db.adapter.enumerable.PhysType;
import org.polypheny.db.adapter.enumerable.PhysTypeImpl;
import org.polypheny.db.adapter.java.JavaTypeFactory;
import org.polypheny.db.algebra.core.document.DocumentValues;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.prepare.JavaTypeFactoryImpl;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.util.BsonUtil;
import org.polypheny.db.util.BuiltInMethod;

public class EnumerableDocumentValues extends DocumentValues implements EnumerableAlg {

    /**
     * Creates a {@link DocumentValues}.
     * {@link ModelTrait#DOCUMENT} node, which contains values.
     *
     * @param cluster
     * @param traitSet
     * @param documentTuples
     */
    public EnumerableDocumentValues( AlgOptCluster cluster, AlgTraitSet traitSet, ImmutableList<BsonValue> documentTuples ) {
        super( cluster, traitSet.replace( EnumerableConvention.INSTANCE ), documentTuples );
    }


    public static EnumerableDocumentValues create( DocumentValues values ) {
        return new EnumerableDocumentValues( values.getCluster(), values.getTraitSet(), values.documentTuples );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        final JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();
        final BlockBuilder builder = new BlockBuilder();
        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getRowType(),
                        pref.preferCustom() );
        final Type rowClass = physType.getJavaRowType();

        final List<Expression> expressions = new ArrayList<>();

        for ( BsonValue docs : documentTuples ) {
            expressions.add( BsonUtil.asExpression( docs ) );
        }
        builder.add(
                Expressions.return_(
                        null,
                        Expressions.call(
                                BuiltInMethod.AS_ENUMERABLE.method,
                                Expressions.newArrayInit( Primitive.box( rowClass ), expressions ) ) ) );
        return implementor.result( physType, builder.toBlock() );
    }

}
