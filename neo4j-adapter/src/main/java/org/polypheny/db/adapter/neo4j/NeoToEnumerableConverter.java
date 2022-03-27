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

package org.polypheny.db.adapter.neo4j;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.enumerable.EnumUtils;
import org.polypheny.db.adapter.enumerable.EnumerableAlg;
import org.polypheny.db.adapter.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.adapter.enumerable.JavaRowFormat;
import org.polypheny.db.adapter.enumerable.PhysType;
import org.polypheny.db.adapter.enumerable.PhysTypeImpl;
import org.polypheny.db.adapter.neo4j.util.NeoUtil;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterImpl;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.ConventionTraitDef;
import org.polypheny.db.type.PolyType;

public class NeoToEnumerableConverter extends ConverterImpl implements EnumerableAlg {

    /**
     * Creates a ConverterImpl.
     *
     * @param cluster planner's cluster
     * @param traits the output traits of this converter
     * @param child child alg (provides input traits)
     */
    protected NeoToEnumerableConverter( AlgOptCluster cluster, AlgTraitSet traits, AlgNode child ) {
        super( cluster, ConventionTraitDef.INSTANCE, traits, child );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        final BlockBuilder blockBuilder = new BlockBuilder();
        final NeoRelationalImplementor neoImplementor = new NeoRelationalImplementor();

        neoImplementor.visitChild( 0, getInput() );

        final AlgDataType rowType = getRowType();

        // PhysType is Enumerable Adapter class that maps SQL types (getRowType) with physical Java types (getJavaTypes())
        final PhysType physType = PhysTypeImpl.of( implementor.getTypeFactory(), rowType, pref.prefer( JavaRowFormat.ARRAY ) );

        final Expression table = blockBuilder.append( "table", neoImplementor.getTable().getExpression( NeoEntity.NeoQueryable.class ) );

        final Expression fields = getFields( blockBuilder, rowType, AlgDataType::getPolyType );

        final Expression arrayFields = getFields( blockBuilder, rowType, NeoUtil::getComponentTypeOrParent );

        final Expression statements = neoImplementor.asExpression();

        final Expression enumerable = blockBuilder.append(
                blockBuilder.newName( "enumerable" ),
                Expressions.call(
                        table,
                        NeoMethod.EXECUTE.method, statements, fields, arrayFields ) );

        blockBuilder.add( Expressions.return_( null, enumerable ) );

        return implementor.result( physType, blockBuilder.toBlock() );
    }


    public Expression getFields( BlockBuilder builder, AlgDataType rowType, Function1<AlgDataType, PolyType> typeGetter ) {
        return builder.append(
                builder.newName( "fields" ),
                EnumUtils.constantArrayList( rowType
                        .getFieldList()
                        .stream()
                        .map( f -> typeGetter.apply( f.getType() ) )
                        .collect( Collectors.toList() ), PolyType.class ) );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new NeoToEnumerableConverter( inputs.get( 0 ).getCluster(), traitSet, inputs.get( 0 ) );
    }

}
