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

package org.polypheny.db.adapter.neo4j.rules;

import static org.polypheny.db.adapter.neo4j.util.NeoUtil.needsGraphValueDecoding;

import java.util.function.Predicate;
import org.polypheny.db.adapter.neo4j.NeoConvention;
import org.polypheny.db.adapter.neo4j.NeoGraph;
import org.polypheny.db.adapter.neo4j.NeoToEnumerableConverterRule;
import org.polypheny.db.adapter.neo4j.rules.graph.NeoLpgAggregate;
import org.polypheny.db.adapter.neo4j.rules.graph.NeoLpgFilter;
import org.polypheny.db.adapter.neo4j.rules.graph.NeoLpgMatch;
import org.polypheny.db.adapter.neo4j.rules.graph.NeoLpgModify;
import org.polypheny.db.adapter.neo4j.rules.graph.NeoLpgProject;
import org.polypheny.db.adapter.neo4j.rules.graph.NeoLpgSort;
import org.polypheny.db.adapter.neo4j.rules.graph.NeoLpgUnwind;
import org.polypheny.db.adapter.neo4j.rules.graph.NeoLpgValues;
import org.polypheny.db.adapter.neo4j.util.NeoUtil.NeoSupportVisitor;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.convert.ConverterRule;
import org.polypheny.db.algebra.core.AlgFactories;
import org.polypheny.db.algebra.core.lpg.LpgAggregate;
import org.polypheny.db.algebra.core.lpg.LpgFilter;
import org.polypheny.db.algebra.core.lpg.LpgMatch;
import org.polypheny.db.algebra.core.lpg.LpgModify;
import org.polypheny.db.algebra.core.lpg.LpgProject;
import org.polypheny.db.algebra.core.lpg.LpgSort;
import org.polypheny.db.algebra.core.lpg.LpgUnwind;
import org.polypheny.db.algebra.core.lpg.LpgValues;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgMatch;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.plan.volcano.AlgSubset;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.graph.GraphPropertyHolder;
import org.polypheny.db.type.entity.graph.PolyPath;

public interface NeoGraphRules {

    AlgOptRule[] RULES = {
            NeoToEnumerableConverterRule.INSTANCE,
            NeoGraphModifyRule.INSTANCE,
            NeoGraphProjectRule.INSTANCE,
            NeoGraphFilterRule.INSTANCE,
            NeoGraphValuesRule.INSTANCE,
            NeoGraphSortRule.INSTANCE,
            NeoGraphUnwindRule.INSTANCE,
            NeoGraphAggregateRule.INSTANCE,
            NeoGraphMatchRule.INSTANCE
    };

    static boolean supports( LpgProject r ) {
        if ( containsSort( r.getInput() ) ) {
            return false;
        }
        NeoSupportVisitor visitor = new NeoSupportVisitor( true );
        for ( RexNode project : r.getProjects() ) {
            if ( project instanceof RexLiteral literal && literal.isNull() ) {
                // Native projection expects a string expression, but the literal translator returns null.
                return false;
            }
            project.accept( visitor );
        }
        return visitor.isSupports();
    }

    static boolean supports( LpgFilter r ) {
        if ( containsSort( r.getInput() ) || needsGraphValueDecoding( r.getCondition().getType().getPolyType() ) ) {
            return false;
        }
        NeoSupportVisitor visitor = new NeoSupportVisitor( true );
        r.getCondition().accept( visitor );
        return visitor.isSupports();
    }


    private static boolean containsSort( AlgNode input ) {
        AlgNode node = input instanceof AlgSubset subset ? subset.getOriginal() : input;
        // NeoLpgSort emits RETURN followed by ORDER BY/SKIP/LIMIT. Additional clauses need a new query stage.
        return node instanceof LpgSort || node.getInputs().stream().anyMatch( NeoGraphRules::containsSort );
    }


    private static boolean supportsPatternProperties( GraphPropertyHolder holder ) {
        // An exact decimal may be stored as a tagged string while still comparing equal to a native number.
        return holder.properties.values().stream().allMatch( value -> value != null && !value.isNull() && (value.isString() || value.isBoolean()) );
    }


    abstract class NeoConverterRule extends ConverterRule {

        protected final Convention out;


        public <R extends AlgNode> NeoConverterRule( Class<R> clazz, Predicate<? super R> supports, String description ) {
            super( clazz, supports, Convention.NONE, NeoConvention.INSTANCE, AlgFactories.LOGICAL_BUILDER, description );
            this.out = NeoConvention.INSTANCE;
        }

    }


    class NeoGraphModifyRule extends NeoConverterRule {

        public static NeoGraphModifyRule INSTANCE = new NeoGraphModifyRule( LpgModify.class, r -> true, NeoGraphModifyRule.class.getSimpleName() );


        private <R extends AlgNode> NeoGraphModifyRule( Class<R> clazz, Predicate<? super R> supports, String description ) {
            super( clazz, supports, description );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            LpgModify<?> mod = (LpgModify<?>) alg;

            if ( !(mod.entity instanceof NeoGraph) ) {
                return null;
            }

            LpgModify<NeoGraph> modify = (LpgModify<NeoGraph>) alg;
            return new NeoLpgModify(
                    modify.getCluster(),
                    modify.getTraitSet().replace( NeoConvention.INSTANCE ),
                    modify.getEntity(),
                    convert( modify.getInput(), NeoConvention.INSTANCE ),
                    modify.operation,
                    modify.ids,
                    modify.operations );
        }

    }


    class NeoGraphProjectRule extends NeoConverterRule {

        public static NeoGraphProjectRule INSTANCE = new NeoGraphProjectRule( LpgProject.class, NeoGraphRules::supports, "NeoGraphProjectRule" );


        private <R extends AlgNode> NeoGraphProjectRule( Class<R> clazz, Predicate<? super R> supports, String description ) {
            super( clazz, supports, description );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            LpgProject project = (LpgProject) alg;
            return new NeoLpgProject(
                    alg.getCluster(),
                    alg.getTraitSet().replace( NeoConvention.INSTANCE ),
                    convert( project.getInput(), NeoConvention.INSTANCE ),
                    project.getNames(),
                    project.getProjects() );
        }

    }


    class NeoGraphFilterRule extends NeoConverterRule {

        public static NeoGraphFilterRule INSTANCE = new NeoGraphFilterRule( LpgFilter.class, NeoGraphRules::supports, "NeoGraphFilterRule" );


        private <R extends AlgNode> NeoGraphFilterRule( Class<R> clazz, Predicate<? super R> supports, String description ) {
            super( clazz, supports, description );
        }


        @Override
        public LpgFilter convert( AlgNode alg ) {
            LpgFilter filter = (LpgFilter) alg;
            return new NeoLpgFilter(
                    filter.getCluster(),
                    filter.getTraitSet().replace( NeoConvention.INSTANCE ),
                    convert( filter.getInput(), NeoConvention.INSTANCE ),
                    filter.getCondition() );
        }

    }


    class NeoGraphValuesRule extends NeoConverterRule {

        public static NeoGraphValuesRule INSTANCE = new NeoGraphValuesRule( LpgValues.class, r -> true, "NeoGraphValuesRule" );


        private <R extends AlgNode> NeoGraphValuesRule( Class<R> clazz, Predicate<? super R> supports, String description ) {
            super( clazz, supports, description );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            LpgValues values = (LpgValues) alg;
            return new NeoLpgValues(
                    values.getCluster(),
                    values.getTraitSet().replace( NeoConvention.INSTANCE ),
                    values.getNodes(),
                    values.getEdges(),
                    values.getValues(),
                    values.getTupleType() );
        }

    }


    class NeoGraphSortRule extends NeoConverterRule {

        public static NeoGraphSortRule INSTANCE = new NeoGraphSortRule( LpgSort.class, NeoGraphSortRule::supports, "NeoGraphSortRule" );


        private static boolean supports( LpgSort sort ) {
            return !containsSort( sort.getInput() ) && sort.collation.getFieldCollations().stream().noneMatch( field ->
                    needsGraphValueDecoding( sort.getInput().getTupleType().getFields().get( field.getFieldIndex() ).getType().getPolyType() ) );
        }


        private <R extends AlgNode> NeoGraphSortRule( Class<R> clazz, Predicate<? super R> supports, String description ) {
            super( clazz, supports, description );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            LpgSort sort = (LpgSort) alg;
            return new NeoLpgSort(
                    sort.getCluster(),
                    sort.getTraitSet().replace( NeoConvention.INSTANCE ),
                    convert( sort.getInput(), NeoConvention.INSTANCE ),
                    sort.collation,
                    sort.offset,
                    sort.fetch );
        }

    }


    class NeoGraphUnwindRule extends NeoConverterRule {

        public static NeoGraphUnwindRule INSTANCE = new NeoGraphUnwindRule( LpgUnwind.class, unwind -> !containsSort( unwind.getInput() )
                && unwind.getInput().getTupleType().getFields().get( unwind.index ).getType().getPolyType() != PolyType.ANY, "NeoGraphUnwindRule" );


        private <R extends AlgNode> NeoGraphUnwindRule( Class<R> clazz, Predicate<? super R> supports, String description ) {
            super( clazz, supports, description );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            LpgUnwind sort = (LpgUnwind) alg;
            return new NeoLpgUnwind(
                    sort.getCluster(),
                    sort.getTraitSet().replace( NeoConvention.INSTANCE ),
                    convert( sort.getInput(), NeoConvention.INSTANCE ),
                    sort.index,
                    sort.alias );
        }

    }


    class NeoGraphAggregateRule extends NeoConverterRule {

        public static NeoGraphAggregateRule INSTANCE = new NeoGraphAggregateRule(
                LpgAggregate.class, NeoGraphAggregateRule::supports, "NeoGraphAggregateRule" );


        private static boolean supports( LpgAggregate aggregate ) {
            if ( aggregate.aggCalls.stream().anyMatch( call -> call.isDistinct() || call.function.getKind() != Kind.COUNT ) ) {
                return false;
            }
            if ( aggregate.getTupleType().getFields().stream().anyMatch( field -> field.getType().getPolyType() == PolyType.DECIMAL
                    || !field.getName().matches( "[A-Za-z_][A-Za-z0-9_]*" ) )
                    || aggregate.groups.stream().anyMatch( group -> needsGraphValueDecoding( group.getType().getPolyType() ) ) ) {
                return false;
            }
            return !containsSort( aggregate.getInput() );
        }


        private <R extends AlgNode> NeoGraphAggregateRule( Class<R> clazz, Predicate<? super R> supports, String description ) {
            super( clazz, supports, description );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            LpgAggregate aggregate = (LpgAggregate) alg;
            return new NeoLpgAggregate(
                    aggregate.getCluster(),
                    aggregate.getTraitSet().replace( NeoConvention.INSTANCE ),
                    convert( aggregate.getInput(), NeoConvention.INSTANCE ),
                    aggregate.groups,
                    aggregate.aggCalls,
                    aggregate.getTupleType() );
        }

    }


    class NeoGraphMatchRule extends NeoConverterRule {

        public static NeoGraphMatchRule INSTANCE = new NeoGraphMatchRule( LogicalLpgMatch.class, NeoGraphMatchRule::supports, NeoGraphMatchRule.class.getSimpleName() );


        private static boolean supports( LogicalLpgMatch match ) {
            return !match.isEnumerableOnly() && !containsSort( match.getInput() )
                    && match.getMatches().stream().flatMap( call -> call.operands.stream() ).allMatch( operand -> {
                        if ( operand instanceof RexLiteral literal && literal.value instanceof GraphPropertyHolder holder ) {
                            return supportsPatternProperties( holder );
                        }
                        if ( operand instanceof RexLiteral literal && literal.value instanceof PolyPath path ) {
                            return supportsPath( path );
                        }
                        return true;
                    } );
        }


        private static boolean supportsPath( PolyPath path ) {
            if ( !path.getPath().stream().allMatch( NeoGraphRules::supportsPatternProperties ) ) {
                return false;
            }
            // A native path carries its nodes and relationships, but cannot reconstruct variable
            // bindings at an enumerable boundary when a segment has zero or an open number of hops.
            boolean hasBindings = path.getNames().stream().anyMatch( NeoGraphMatchRule::isNamed );
            return path.getEdges().stream().filter( edge -> edge.fromTo() != null ).noneMatch( edge ->
                    isNamed( edge.getVariableName() ) || edge.labels.size() > 1
                            || (hasBindings && (edge.getMinLength() == 0 || edge.getMaxLength() == null)) );
        }


        private static boolean isNamed( PolyString name ) {
            return name != null && !name.isNull();
        }


        private <R extends AlgNode> NeoGraphMatchRule( Class<R> clazz, Predicate<? super R> supports, String description ) {
            super( clazz, supports, description );
        }


        @Override
        public AlgNode convert( AlgNode alg ) {
            LpgMatch match = (LpgMatch) alg;
            return new NeoLpgMatch(
                    match.getCluster(),
                    match.getTraitSet().replace( NeoConvention.INSTANCE ),
                    convert( match.getInput(), NeoConvention.INSTANCE ),
                    match.getMatches(),
                    match.getNames() );
        }

    }

}
