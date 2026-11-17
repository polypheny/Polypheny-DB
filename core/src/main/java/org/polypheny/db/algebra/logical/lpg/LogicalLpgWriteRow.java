/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.algebra.logical.lpg;

import java.util.List;
import javax.annotation.Nullable;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.lpg.LpgAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.enumerable.JavaTupleFormat;
import org.polypheny.db.algebra.enumerable.PhysTypeImpl;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.BuiltInMethod;


/** Captures projected rows before UPDATE or appends an identity seed for CREATE. */
public class LogicalLpgWriteRow extends SingleAlg implements LpgAlg {

    public static final ConverterRule TO_ENUMERABLE = new ConverterRule(
            LogicalLpgWriteRow.class, node -> true, Convention.NONE, EnumerableConvention.INSTANCE,
            AlgFactories.LOGICAL_BUILDER, "LpgWriteRowToEnumerable" ) {
        @Override
        public AlgNode convert( AlgNode node ) {
            LogicalLpgWriteRow row = (LogicalLpgWriteRow) node;
            AlgNode input = convert( row.getInput(), row.getInput().getTraitSet().replace( EnumerableConvention.INSTANCE ) );
            return new EnumerableWriteRow( row.getTraitSet().replace( EnumerableConvention.INSTANCE ), input, row.name );
        }
    };

    @Nullable
    private final String name;


    public LogicalLpgWriteRow( AlgNode input ) {
        this( input, null );
    }


    public LogicalLpgWriteRow( AlgNode input, @Nullable String name ) {
        this( input.getTraitSet(), input, name );
    }


    protected LogicalLpgWriteRow( AlgTraitSet traits, AlgNode input, @Nullable String name ) {
        super( input.getCluster(), traits, input );
        this.name = name;
    }


    @Override
    protected AlgDataType deriveRowType() {
        if ( name == null ) {
            return input.getTupleType();
        }
        return getCluster().getTypeFactory().builder()
                .addAll( input.getTupleType().getFields() )
                .add( name, null, PolyType.VARCHAR, 36 )
                .build();
    }


    @Override
    public NodeType getNodeType() {
        return NodeType.PROJECT;
    }


    @Override
    public String algCompareString() {
        return getClass().getSimpleName() + "$" + name + "$" + input.algCompareString() + "&";
    }


    @Override
    public AlgNode copy( AlgTraitSet traits, List<AlgNode> inputs ) {
        return new LogicalLpgWriteRow( traits, inputs.get( 0 ), name );
    }


    private static final class EnumerableWriteRow extends LogicalLpgWriteRow implements EnumerableAlg {

        @Nullable
        private final String idName;


        private EnumerableWriteRow( AlgTraitSet traits, AlgNode input, @Nullable String name ) {
            super( traits, input, name );
            this.idName = name;
        }


        @Override
        public AlgNode copy( AlgTraitSet traits, List<AlgNode> inputs ) {
            return new EnumerableWriteRow( traits, inputs.get( 0 ), idName );
        }


        @Override
        public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
            Result child = implementor.visitChild( this, 0, (EnumerableAlg) getInput(), idName == null ? pref : Prefer.ARRAY );
            BlockBuilder builder = new BlockBuilder();
            Expression rows = builder.append( "rows", child.block() );
            if ( idName == null ) {
                builder.add( Expressions.return_( null, Expressions.call( BuiltInMethod.MATERIALIZE_GRAPH_ROWS.method, rows ) ) );
                return implementor.result( child.physType(), builder.toBlock() );
            }
            builder.add( Expressions.return_( null, Expressions.call( BuiltInMethod.APPEND_GRAPH_ROW_ID.method, rows ) ) );
            return implementor.result( PhysTypeImpl.of( implementor.getTypeFactory(), getTupleType(), JavaTupleFormat.ARRAY, false ), builder.toBlock() );
        }

    }

}
