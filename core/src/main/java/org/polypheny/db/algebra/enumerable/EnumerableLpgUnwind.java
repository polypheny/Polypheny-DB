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

package org.polypheny.db.algebra.enumerable;

import java.util.List;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.lpg.LpgUnwind;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.type.entity.PolyValue;


public class EnumerableLpgUnwind extends LpgUnwind implements EnumerableAlg {

    /**
     * Creates a {@link EnumerableLpgUnwind}.
     */
    protected EnumerableLpgUnwind( AlgCluster cluster, AlgTraitSet traits, AlgNode input, int index, String alias ) {
        super( cluster, traits, input, index, alias );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 2 );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new EnumerableLpgUnwind( inputs.get( 0 ).getCluster(), traitSet, inputs.get( 0 ), index, alias );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        Result child = implementor.visitChild( this, 0, (EnumerableAlg) input, Prefer.ARRAY );
        BlockBuilder builder = new BlockBuilder();
        Expression rows = builder.append( "rows", child.block() );
        builder.add( Expressions.return_( null, Expressions.call(
                EnumerableLpgUnwind.class, "expand", rows, Expressions.constant( index ) ) ) );
        PhysType type = PhysTypeImpl.of( implementor.getTypeFactory(), getTupleType(), JavaTupleFormat.ARRAY, false );
        return implementor.result( type, builder.toBlock() );
    }


    public static Enumerable<PolyValue[]> expand( Enumerable<PolyValue[]> rows, int index ) {
        return rows.selectMany( row -> {
            PolyValue value = row[index];
            if ( value == null || value.isNull() ) {
                return Linq4j.emptyEnumerable();
            }
            List<? extends PolyValue> items = value.isList() ? value.asList() : List.of( value );
            return Linq4j.asEnumerable( items ).select( item -> {
                PolyValue[] result = new PolyValue[row.length];
                System.arraycopy( row, 0, result, 0, index );
                System.arraycopy( row, index + 1, result, index, row.length - index - 1 );
                result[row.length - 1] = item;
                return result;
            } );
        } );
    }

}
