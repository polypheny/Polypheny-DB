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

package org.polypheny.db.algebra.enumerable.document;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Primitive;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.core.document.DocumentValues;
import org.polypheny.db.algebra.enumerable.EnumerableAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.enumerable.PhysType;
import org.polypheny.db.algebra.enumerable.PhysTypeImpl;
import org.polypheny.db.functions.CrossModelFunctions;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.util.BuiltInMethod;

public class EnumerableDocumentValues extends DocumentValues implements EnumerableAlg {

    /**
     * Creates a {@link DocumentValues}.
     * {@link ModelTrait#DOCUMENT} node, which contains values.
     *
     * @param cluster
     * @param traitSet
     * @param document
     */
    public EnumerableDocumentValues( AlgCluster cluster, AlgTraitSet traitSet, List<PolyDocument> document, List<RexDynamicParam> dynamicDocuments ) {
        super( cluster, traitSet.replace( EnumerableConvention.INSTANCE ), document, dynamicDocuments );
    }


    public static EnumerableDocumentValues create( DocumentValues values ) {
        return new EnumerableDocumentValues( values.getCluster(), values.getTraitSet(), values.documents, values.dynamicDocuments );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        final BlockBuilder builder = new BlockBuilder();
        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getTupleType(),
                        pref.preferCustom() );

        final List<Expression> expressions = new ArrayList<>();

        if ( isPrepared() ) {
            return attachPreparedExpression( builder, physType, implementor, pref );
        }

        for ( PolyValue doc : documents ) {
            expressions.add( Expressions.newArrayInit( PolyDocument.class, doc.asExpression() ) );
        }
        builder.add(
                Expressions.return_(
                        null,
                        Expressions.call(
                                BuiltInMethod.AS_ENUMERABLE.method,
                                Expressions.newArrayInit( Primitive.box( PolyValue.class ), 2, expressions ) ) ) );
        return implementor.result( physType, builder.toBlock() );
    }


    private Result attachPreparedExpression( BlockBuilder builder, PhysType physType, EnumerableAlgImplementor implementor, Prefer pref ) {

        builder.add(
                Expressions.return_(
                        null,
                        Expressions.call(
                                BuiltInMethod.AS_ENUMERABLE.method,
                                Expressions.call( CrossModelFunctions.class, "enumerableFromContext", DataContext.ROOT ) ) ) );
        return implementor.result( physType, builder.toBlock() );
    }

}
