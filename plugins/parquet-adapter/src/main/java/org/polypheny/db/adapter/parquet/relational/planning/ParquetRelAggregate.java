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

package org.polypheny.db.adapter.parquet.relational.planning;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.parquet.relational.schema.ParquetRelTable;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.enumerable.EnumUtils;
import org.polypheny.db.algebra.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.algebra.fun.AggFunction;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.polyalg.arguments.ListArg;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArgs;
import org.polypheny.db.algebra.polyalg.arguments.StringArg;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.ImmutableBitSet;


/**
 * Parquet-convention aggregate. The node has a single visible planner shape and selects the concrete aggregate execution mode internally.
 */
public class ParquetRelAggregate extends SingleAlg implements ParquetAlg {

    private static final int NO_ARGUMENT = -1;

    private final ParquetRelTable table;
    private final int[] fields;
    private final List<ParquetAdapterFilter<PolyValue>> filters;
    private final AggregateMode mode;
    private final int[] groupFields;
    private final String[] aggregateKinds;
    private final int[] aggregateArgs;
    private final AlgDataType rowType;


    public static ParquetRelAggregate create( ParquetRelScan scan, Aggregate aggregate ) {
        if ( aggregate.indicator || !Aggregate.isSimple( aggregate ) ) {
            return null;
        }
        AggregateMode mode = aggregateMode( scan, aggregate.getAggCallList(), aggregate.getGroupSet() );
        if ( mode == null ) {
            return null;
        }
        return new ParquetRelAggregate(
                aggregate.getCluster(),
                aggregate.getCluster().traitSetOf( ParquetConvention.INSTANCE ).replace( ModelTrait.RELATIONAL ),
                mode == AggregateMode.METADATA ? new ParquetRelMetadataScan( scan ) : scan,
                scan.getEntity(),
                scan.getFields(),
                scan.getFilters(),
                mode,
                aggregate.getGroupSet().asList().stream().mapToInt( Integer::intValue ).toArray(),
                aggregateKinds( aggregate.getAggCallList() ),
                aggregateArgs( aggregate.getAggCallList() ),
                aggregate.getTupleType() );
    }


    private ParquetRelAggregate(
            AlgCluster cluster,
            AlgTraitSet traitSet,
            AlgNode input,
            ParquetRelTable table,
            int[] fields,
            List<ParquetAdapterFilter<PolyValue>> filters,
            AggregateMode mode,
            int[] groupFields,
            String[] aggregateKinds,
            int[] aggregateArgs,
            AlgDataType rowType ) {
        super( cluster, traitSet.replace( ParquetConvention.INSTANCE ).replace( ModelTrait.RELATIONAL ), input );
        this.table = table;
        this.fields = fields.clone();
        this.filters = List.copyOf( filters );
        this.mode = mode;
        this.groupFields = groupFields.clone();
        this.aggregateKinds = aggregateKinds.clone();
        this.aggregateArgs = aggregateArgs.clone();
        this.rowType = rowType;
    }


    /**
     * Validates if the provided AGGREGATE node contains only aggregation functions, filters and group fields supported by metadata pushdown.
     *
     * @param scan a scan that should support the aggregate.
     * @param aggregate an aggregate to validate.
     * @return {@code true} if the AGGREGATE node contains only supported metadata aggregation functions, filters and group fields and {@code false} otherwise.
     */
    public static boolean supports( ParquetRelScan scan, Aggregate aggregate ) {
        if ( aggregate.indicator || !Aggregate.isSimple( aggregate ) ) {
            return false;
        }
        return aggregateMode( scan, aggregate.getAggCallList(), aggregate.getGroupSet() ) != null;
    }


    /**
     * Validates if a single aggregate call can be pushed into metadata aggregation.
     *
     * @param aggregateCall an aggregate call to check.
     * @return {@code true} if the aggregate call is supported and {@code false} otherwise.
     */
    public static boolean supportsMetadataAggregateCall( AggregateCall aggregateCall ) {
        if ( aggregateCall.isDistinct() || aggregateCall.isApproximate() || aggregateCall.filterArg >= 0 ) {
            return false;
        }
        AggFunction aggregation = aggregateCall.getAggregation();
        if ( aggregation.getKind() == Kind.COUNT ) {
            return aggregateCall.getArgList().size() <= 1;
        }
        if ( aggregation.getKind() == Kind.MIN || aggregation.getKind() == Kind.MAX ) {
            return aggregateCall.getArgList().size() == 1;
        }
        return false;
    }


    /**
     * Checks the aggregate mode: metadata or streaming.
     *
     * @param scan a scan representing the parquet file
     * @param aggregateCalls a list of aggregate functions
     * @param groupSet a set of group by fields.
     * @return {@link AggregateMode} if one of them is supported and {@code null} otherwise.
     */
    private static AggregateMode aggregateMode( ParquetRelScan scan, List<AggregateCall> aggregateCalls, ImmutableBitSet groupSet ) {
        boolean metadataCandidate = true;
        for ( AggregateCall aggregateCall : aggregateCalls ) {
            if ( !supportsMetadataAggregateCall( aggregateCall ) ) {
                metadataCandidate = false;
                break;
            }
        }
        if ( metadataCandidate && scan.getEntity().supportsMetadataAggregate( scan.getFields(), scan.getFilters(), groupSet, aggregateCalls ) ) {
            return AggregateMode.METADATA;
        }
        if ( scan.getEntity().supportsDataAggregate( scan.getFields(), groupSet, aggregateCalls ) ) {
            return AggregateMode.DATA;
        }
        return null;
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new ParquetRelAggregate( getCluster(), traitSet, inputs.get( 0 ), table, fields, filters, mode, groupFields, aggregateKinds, aggregateArgs, rowType );
    }


    @Override
    protected AlgDataType deriveRowType() {
        return rowType;
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw )
                .item( "mode", modeName() )
                .item( "fields", fieldNames() )
                .item( "groups", groupNames() )
                .item( "aggregates", aggregateNames() )
                .item( "filters", filterNames() );
    }


    @Override
    public String algCompareString() {
        return getClass().getSimpleName()
                + "$input=" + input.algCompareString()
                + "$mode=" + modeName()
                + "$fields=" + fieldNames()
                + "$groups=" + groupNames()
                + "$aggregates=" + aggregateNames()
                + "$filters=" + filterNames();
    }


    @Override
    public PolyAlgArgs bindArguments() {
        return super.bindArguments()
                .put( "mode", new StringArg( modeName() ) )
                .put( "fields", new ListArg<>( fieldNames(), StringArg::new ) )
                .put( "groups", new ListArg<>( groupNames(), StringArg::new ) )
                .put( "aggregates", new ListArg<>( aggregateNames(), StringArg::new ) )
                .put( "filters", new ListArg<>( filterNames(), StringArg::new ) );
    }


    @Override
    public void register( AlgPlanner planner ) {
        ParquetConvention.INSTANCE.register( planner );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( ParquetConvention.COST_MULTIPLIER * 0.01D );
    }


    @Override
    public Expression implement( EnumerableAlgImplementor implementor ) {
        Expression runtimeFilters = EnumUtils.expressionList( filters.stream().map( ParquetAdapterFilter::toExpression ).toList() );
        return Expressions.call(
                table.asExpression( ParquetRelTable.class ),
                mode == AggregateMode.METADATA ? "metadataAggregate" : "dataAggregate",
                implementor.getRootExpression(),
                Expressions.constant( fields ),
                runtimeFilters,
                Expressions.constant( groupFields ),
                Expressions.constant( aggregateKinds ),
                Expressions.constant( aggregateArgs ) );
    }


    private static String[] aggregateKinds( List<AggregateCall> aggregateCalls ) {
        return aggregateCalls.stream()
                .map( aggregateCall -> aggregateCall.getAggregation().getKind().name() )
                .toArray( String[]::new );
    }


    private static int[] aggregateArgs( List<AggregateCall> aggregateCalls ) {
        return aggregateCalls.stream()
                .mapToInt( aggregateCall -> aggregateCall.getArgList().isEmpty() ? NO_ARGUMENT : aggregateCall.getArgList().get( 0 ) )
                .toArray();
    }


    private String modeName() {
        return switch ( mode ) {
            case METADATA -> "metadataAggregate";
            case DATA -> "dataAggregate";
        };
    }


    private List<String> fieldNames() {
        return ParquetPolyAlgDisplay.fieldNames( table, fields );
    }


    private List<String> groupNames() {
        List<String> projectedFieldNames = fieldNames();
        return Arrays.stream( groupFields )
                .mapToObj( index -> index >= 0 && index < projectedFieldNames.size() ? projectedFieldNames.get( index ) : "#" + index )
                .toList();
    }


    private List<String> aggregateNames() {
        List<String> fieldNames = fieldNames();
        List<String> values = new ArrayList<>( aggregateKinds.length );
        for ( int i = 0; i < aggregateKinds.length; i++ ) {
            int aggregateArg = aggregateArgs[i];
            String argument = aggregateArg == NO_ARGUMENT ? "*" : aggregateArg >= 0 && aggregateArg < fieldNames.size() ? fieldNames.get( aggregateArg ) : "#" + aggregateArg;
            values.add( aggregateKinds[i] + "(" + argument + ")" );
        }
        return values;
    }


    private List<String> filterNames() {
        return ParquetPolyAlgDisplay.filters( filters, ParquetPolyAlgDisplay.fieldNames( table ) );
    }


    private enum AggregateMode {
        /**
         * The aggregation is done only on parquet metadata, no data rows are read.
         */
        METADATA,
        /**
         * The aggregation is done by reading rows with direct field references.
         */
        DATA
    }

}
