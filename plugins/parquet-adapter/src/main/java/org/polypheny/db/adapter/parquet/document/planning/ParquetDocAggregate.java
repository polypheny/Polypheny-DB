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

package org.polypheny.db.adapter.parquet.document.planning;

import java.util.Arrays;
import java.util.List;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.parquet.shared.filter.ParquetAdapterFilter;
import org.polypheny.db.adapter.parquet.shared.optimization.aggregate.ParquetAggregateSupport;
import org.polypheny.db.adapter.parquet.shared.planning.ParquetPolyAlgDisplay;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.SingleAlg;
import org.polypheny.db.algebra.core.Aggregate;
import org.polypheny.db.algebra.core.AggregateCall;
import org.polypheny.db.algebra.enumerable.EnumUtils;
import org.polypheny.db.algebra.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.polyalg.arguments.ListArg;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArgs;
import org.polypheny.db.algebra.polyalg.arguments.StringArg;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;

/**
 * Parquet-convention aggregate for document scans after field extraction has been mapped to primitive Parquet columns.
 */
public class ParquetDocAggregate extends SingleAlg implements org.polypheny.db.adapter.parquet.relational.planning.ParquetAlg {

    private static final int NO_ARGUMENT = -1;

    private final ParquetDocScan scan;
    private final int[] fields;
    private final AggregateMode mode;
    private final int[] groupFields;
    private final String[] aggregateKinds;
    private final int[] aggregateArgs;
    private final AlgDataType rowType;


    private ParquetDocAggregate( AlgCluster cluster, AlgTraitSet traitSet, AlgNode input, ParquetDocScan scan, int[] fields, AggregateMode mode, int[] groupFields, String[] aggregateKinds, int[] aggregateArgs, AlgDataType rowType ) {
        super( cluster, traitSet, input );
        this.scan = scan;
        this.fields = fields.clone();
        this.mode = mode;
        this.groupFields = groupFields.clone();
        this.aggregateKinds = aggregateKinds.clone();
        this.aggregateArgs = aggregateArgs.clone();
        this.rowType = rowType;
    }


    public static ParquetDocAggregate create( ParquetDocScan scan, Aggregate aggregate, int[] fields ) {
        if ( aggregate.indicator || !Aggregate.isSimple( aggregate ) ) {
            return null;
        }
        int[] groupFields = aggregate.getGroupSet().asList().stream().mapToInt( Integer::intValue ).toArray();
        List<AggregateCall> aggregateCalls = normalizeDocumentRootCounts( fields, aggregate.getAggCallList() );
        AggregateMode mode = aggregateMode( scan, fields, aggregateCalls, aggregate.getGroupSet() );
        if ( mode == null ) {
            return null;
        }
        return new ParquetDocAggregate(
                aggregate.getCluster(),
                aggregate.getCluster().traitSetOf( ParquetDocConvention.INSTANCE ),
                mode == AggregateMode.METADATA ? new ParquetDocMetadataScan( scan ) : scan,
                scan,
                fields,
                mode,
                groupFields,
                aggregateKinds( aggregateCalls ),
                aggregateArgs( aggregateCalls ),
                aggregate.getTupleType() );
    }


    private static AggregateMode aggregateMode( ParquetDocScan scan, int[] fields, List<AggregateCall> aggregateCalls, org.polypheny.db.util.ImmutableBitSet groupSet ) {
        boolean metadataCandidate = true;
        for ( AggregateCall aggregateCall : aggregateCalls ) {
            if ( !ParquetAggregateSupport.supportsMetadataAggregateCall( aggregateCall ) ) {
                metadataCandidate = false;
                break;
            }
        }
        if ( metadataCandidate && scan.getEntity().supportsMetadataAggregate( fields, scan.getFilters(), groupSet, aggregateCalls ) ) {
            return AggregateMode.METADATA;
        }
        if ( scan.getEntity().supportsDataAggregate( fields, groupSet, aggregateCalls ) ) {
            return AggregateMode.DATA;
        }
        return null;
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


    private static List<AggregateCall> normalizeDocumentRootCounts( int[] fields, List<AggregateCall> aggregateCalls ) {
        if ( fields.length != 0 ) {
            return aggregateCalls;
        }
        return aggregateCalls.stream()
                .map( aggregateCall -> isDocumentRootCount( aggregateCall )
                        ? aggregateCall.copy( List.of(), aggregateCall.filterArg, aggregateCall.getCollation() )
                        : aggregateCall )
                .toList();
    }


    private static boolean isDocumentRootCount( AggregateCall aggregateCall ) {
        if ( aggregateCall.getAggregation().getKind() != org.polypheny.db.algebra.constant.Kind.COUNT ) {
            return false;
        }
        return aggregateCall.getArgList().isEmpty()
                || (aggregateCall.getArgList().size() == 1 && aggregateCall.getArgList().get( 0 ) == 0);
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new ParquetDocAggregate( getCluster(), traitSet, inputs.get( 0 ), scan, fields, mode, groupFields, aggregateKinds, aggregateArgs, rowType );
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
        ParquetDocConvention.INSTANCE.register( planner );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( ParquetDocConvention.INSTANCE.getCostMultiplier() * 0.01D );
    }


    @Override
    public Expression implement( EnumerableAlgImplementor implementor ) {
        Expression runtimeFilters = EnumUtils.expressionList( scan.getFilters().stream().map( ParquetAdapterFilter::toExpression ).toList() );
        return Expressions.call(
                scan.getEntity().asExpression(),
                mode == AggregateMode.METADATA ? "metadataAggregate" : "dataAggregate",
                implementor.getRootExpression(),
                Expressions.constant( fields ),
                runtimeFilters,
                Expressions.constant( groupFields ),
                Expressions.constant( aggregateKinds ),
                Expressions.constant( aggregateArgs ) );
    }


    private List<String> fieldNames() {
        return ParquetPolyAlgDisplay.fieldNames( scan.getEntity(), fields );
    }


    private String modeName() {
        return switch ( mode ) {
            case METADATA -> "metadataAggregate";
            case DATA -> "dataAggregate";
        };
    }


    private List<String> groupNames() {
        List<String> fieldNames = fieldNames();
        return Arrays.stream( groupFields )
                .mapToObj( index -> index >= 0 && index < fieldNames.size() ? fieldNames.get( index ) : "#" + index )
                .toList();
    }


    private List<String> aggregateNames() {
        List<String> fieldNames = fieldNames();
        java.util.ArrayList<String> values = new java.util.ArrayList<>( aggregateKinds.length );
        for ( int i = 0; i < aggregateKinds.length; i++ ) {
            int aggregateArg = aggregateArgs[i];
            String argument = aggregateArg == NO_ARGUMENT ? "*" : aggregateArg >= 0 && aggregateArg < fieldNames.size() ? fieldNames.get( aggregateArg ) : "#" + aggregateArg;
            values.add( aggregateKinds[i] + "(" + argument + ")" );
        }
        return values;
    }


    private List<String> filterNames() {
        return ParquetPolyAlgDisplay.filters( scan.getFilters(), ParquetPolyAlgDisplay.fieldNames( scan.getEntity() ) );
    }


    private enum AggregateMode {
        METADATA,
        DATA
    }

}
