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

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.calcite.linq4j.function.Function0;
import org.apache.calcite.linq4j.function.Function2;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.lpg.LpgAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.enumerable.JavaTupleFormat;
import org.polypheny.db.algebra.enumerable.PhysType;
import org.polypheny.db.algebra.enumerable.PhysTypeImpl;
import org.polypheny.db.algebra.enumerable.RexToLixTranslator;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.functions.CypherMergeRows;
import org.polypheny.db.functions.CypherMergeRows.Pattern;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.rex.RexShuttle;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.PolyGraph;


/** A graph MERGE clause and its per-row expressions. Routing attaches the placement-specific write plans. */
public class LogicalLpgMergeRows extends AbstractAlgNode implements LpgAlg {

    public static final ConverterRule TO_ENUMERABLE = new ConverterRule(
            LogicalLpgMergeRows.class, node -> node.getInputs().size() >= 3, Convention.NONE, EnumerableConvention.INSTANCE,
            AlgFactories.LOGICAL_BUILDER, "LpgMergeRowsToEnumerable" ) {
        @Override
        public AlgNode convert( AlgNode node ) {
            LogicalLpgMergeRows merge = (LogicalLpgMergeRows) node;
            List<AlgNode> inputs = node.getInputs().stream()
                    .map( input -> convert( input, input.getTraitSet().replace( EnumerableConvention.INSTANCE ) ) )
                    .toList();
            return new EnumerableMergeRows( merge, node.getTraitSet().replace( EnumerableConvention.INSTANCE ), inputs );
        }
    };

    private final List<AlgNode> inputs;
    private final int clauseId;
    protected final Pattern pattern;
    protected final List<RexNode> properties;
    protected final List<Action> onCreate;
    protected final List<Action> onMatch;
    protected final List<Integer> updateTargets;
    protected final boolean countOnly;


    public record Action( int target, RexNode update ) {
    }


    public LogicalLpgMergeRows( AlgNode input, AlgNode scan, AlgDataType rowType, Pattern pattern, List<RexNode> properties,
            List<Action> onCreate, List<Action> onMatch, boolean countOnly ) {
        this( input.getTraitSet(), List.of( input, scan ), rowType, pattern, properties, onCreate, onMatch, countOnly, -1 );
    }


    private LogicalLpgMergeRows( AlgTraitSet traits, List<AlgNode> inputs, AlgDataType rowType, Pattern pattern, List<RexNode> properties,
            List<Action> onCreate, List<Action> onMatch, boolean countOnly, int clauseId ) {
        super( inputs.get( 0 ).getCluster(), traits );
        this.inputs = new ArrayList<>( inputs );
        this.clauseId = clauseId >= 0 ? clauseId : getId();
        this.rowType = rowType;
        this.pattern = pattern;
        this.properties = List.copyOf( properties );
        this.onCreate = List.copyOf( onCreate );
        this.onMatch = List.copyOf( onMatch );
        List<Integer> targets = new ArrayList<>();
        for ( Action action : onCreate ) {
            if ( !targets.contains( action.target ) ) {
                targets.add( action.target );
            }
        }
        for ( Action action : onMatch ) {
            if ( !targets.contains( action.target ) ) {
                targets.add( action.target );
            }
        }
        this.updateTargets = List.copyOf( targets );
        this.countOnly = countOnly;
    }


    public List<PolyType> getInsertTypes() {
        return pattern.insertionTypes();
    }


    public List<PolyType> getUpdateTypes() {
        return updateTargets.stream().map( target -> rowType.getFields().get( target ).getType().getPolyType() ).toList();
    }


    @Override
    public List<AlgNode> getInputs() {
        return List.copyOf( inputs );
    }


    @Override
    public void replaceInput( int ordinal, AlgNode input ) {
        inputs.set( ordinal, input );
        recomputeDigest();
    }


    @Override
    public NodeType getNodeType() {
        return NodeType.MERGE;
    }


    @Override
    public AlgNode copy( AlgTraitSet traits, List<AlgNode> inputs ) {
        return new LogicalLpgMergeRows( traits, inputs, rowType, pattern, properties, onCreate, onMatch, countOnly, clauseId );
    }


    @Override
    public AlgNode accept( RexShuttle shuttle ) {
        return rewriteExpressions( shuttle, shuttle );
    }


    public AlgNode rewriteExpressions( RexShuttle propertyShuttle, RexShuttle actionShuttle ) {
        List<RexNode> newProperties = propertyShuttle.apply( properties );
        List<Action> newOnCreate = onCreate.stream().map( action -> new Action( action.target, action.update.accept( actionShuttle ) ) ).toList();
        List<Action> newOnMatch = onMatch.stream().map( action -> new Action( action.target, action.update.accept( actionShuttle ) ) ).toList();
        if ( newProperties.equals( properties ) && newOnCreate.equals( onCreate ) && newOnMatch.equals( onMatch ) ) {
            return this;
        }
        return new LogicalLpgMergeRows( getTraitSet(), getInputs(), rowType, pattern, newProperties, newOnCreate, newOnMatch, countOnly, clauseId );
    }


    @Override
    public AlgWriter explainTerms( AlgWriter writer ) {
        AlgWriter result = super.explainTerms( writer );
        for ( int i = 0; i < inputs.size(); i++ ) {
            result = result.input( "input" + i, inputs.get( i ) );
        }
        // Keep distinct MERGE clauses separate even when their patterns and actions are identical.
        return result.item( "clauseId", clauseId ).item( "pattern", pattern ).item( "properties", properties )
                .item( "onCreate", onCreate ).item( "onMatch", onMatch ).item( "countOnly", countOnly );
    }


    private static final class EnumerableMergeRows extends LogicalLpgMergeRows implements EnumerableAlg {

        private EnumerableMergeRows( LogicalLpgMergeRows merge, AlgTraitSet traits, List<AlgNode> inputs ) {
            super( traits, inputs, merge.rowType, merge.pattern, merge.properties, merge.onCreate, merge.onMatch, merge.countOnly, merge.clauseId );
        }


        @Override
        public AlgNode copy( AlgTraitSet traits, List<AlgNode> inputs ) {
            return new EnumerableMergeRows( this, traits, inputs );
        }


        @Override
        public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
            BlockBuilder builder = new BlockBuilder();
            Result input = implementor.visitChild( this, 0, (EnumerableAlg) getInput( 0 ), Prefer.ARRAY );
            Expression rows = builder.append( "rows", input.block() );
            PhysType inputArray = PhysTypeImpl.of( implementor.getTypeFactory(), getInput( 0 ).getTupleType(), JavaTupleFormat.ARRAY, false );
            rows = input.physType().convertTo( rows, inputArray );
            Expression graph = childFunction( implementor, 1 );
            Expression insert = childFunction( implementor, 2 );
            Expression update = getInputs().size() == 4 ? childFunction( implementor, 3 ) : Expressions.constant( null, Function0.class );
            PhysType outputType = PhysTypeImpl.of( implementor.getTypeFactory(), getTupleType(), JavaTupleFormat.ARRAY, false );
            Expression evaluateProperties = propertiesFunction( implementor, inputArray );
            Expression onCreateFunction = actionFunction( implementor, outputType, onCreate );
            Expression onMatchFunction = actionFunction( implementor, outputType, onMatch );
            Expression execute = Expressions.call( CypherMergeRows.class, "execute",
                    DataContext.ROOT, rows, graph, implementor.stash( pattern, Pattern.class ), insert, update,
                    Expressions.constant( getUpdateTypes() ), evaluateProperties, onCreateFunction, onMatchFunction,
                    Expressions.constant( countOnly ) );
            // Complete the writes before a downstream MATCH initializes its sibling graph scan.
            builder.add( Expressions.return_( null, builder.append( "mergedRows", execute, false ) ) );
            return implementor.result( outputType, builder.toBlock() );
        }


        private Expression childFunction( EnumerableAlgImplementor implementor, int index ) {
            Result child = implementor.visitChild( this, index, (EnumerableAlg) getInput( index ), Prefer.ARRAY );
            return Expressions.lambda( Function0.class, child.block() );
        }


        private Expression propertiesFunction( EnumerableAlgImplementor implementor, PhysType inputType ) {
            ParameterExpression row = Expressions.parameter( Modifier.FINAL, PolyValue[].class, "input" );
            ParameterExpression graph = Expressions.parameter( Modifier.FINAL, PolyGraph.class, "graph" );
            BlockBuilder builder = new BlockBuilder();
            List<Expression> expressions = translate( implementor, inputType, row, graph, properties, builder );
            builder.add( Expressions.return_( null, Expressions.newArrayInit( PolyValue.class, expressions ) ) );
            return Expressions.lambda( Function2.class, builder.toBlock(), row, graph );
        }


        private Expression actionFunction( EnumerableAlgImplementor implementor, PhysType inputType, List<Action> actions ) {
            ParameterExpression row = Expressions.parameter( Modifier.FINAL, PolyValue[].class, "bindings" );
            ParameterExpression graph = Expressions.parameter( Modifier.FINAL, PolyGraph.class, "graph" );
            BlockBuilder builder = new BlockBuilder();
            Expression updates = builder.append( "updates", Expressions.newArrayBounds( PolyValue.class, 1,
                    Expressions.constant( updateTargets.size() ) ), false );
            for ( Action action : actions ) {
                // Each SET item observes the graph values after all preceding items.
                BlockBuilder item = new BlockBuilder( false );
                Expression value = translate( implementor, inputType, row, graph, List.of( action.update ), item ).get( 0 );
                item.add( Expressions.statement( Expressions.assign(
                        Expressions.arrayIndex( updates, Expressions.constant( updateTargets.indexOf( action.target ) ) ), value ) ) );
                builder.add( item.toBlock() );
            }
            builder.add( Expressions.return_( null, updates ) );
            return Expressions.lambda( Function2.class, builder.toBlock(), row, graph );
        }


        private List<Expression> translate( EnumerableAlgImplementor implementor, PhysType inputType, ParameterExpression row,
                ParameterExpression graph, List<RexNode> expressions, BlockBuilder builder ) {
            List<String> names = IntStream.range( 0, expressions.size() ).mapToObj( i -> "merge" + i ).toList();
            AlgDataType inputRow = inputType.getTupleType();
            int graphIndex = inputRow.getFieldCount();
            // The final Rex input is the current graph and is passed separately from the bindings array.
            AlgDataType graphType = implementor.getTypeFactory().createPolyType( PolyType.GRAPH );
            List<AlgDataTypeField> fields = new ArrayList<>( inputRow.getFields() );
            fields.add( new AlgDataTypeFieldImpl( -1L, "$graph", graphIndex, graphType ) );
            AlgDataType expressionRow = new AlgRecordType( inputRow.getStructKind(), fields );
            RexProgram program = RexProgram.create( expressionRow, expressions, null, names, getCluster().getRexBuilder() );
            PhysType output = PhysTypeImpl.of( implementor.getTypeFactory(), program.getOutputRowType(), JavaTupleFormat.ARRAY, false );
            RexToLixTranslator.InputGetter bindings = new RexToLixTranslator.InputGetterImpl( row, inputType );
            RexToLixTranslator.InputGetter inputs = (block, index, storageType) -> index == graphIndex ? graph : bindings.field( block, index, storageType );
            return RexToLixTranslator.translateProjects( program, implementor.getTypeFactory(), implementor.getConformance(), builder,
                    output, DataContext.ROOT, inputs, null );
        }

    }

}
