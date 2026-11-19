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

package org.polypheny.db.algebra.enumerable;


import java.util.List;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.polyalg.PolyAlgMetadata;
import org.polypheny.db.algebra.polyalg.PolyAlgUtils;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArgs;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexLocalRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexProgram;
import org.polypheny.db.rex.RexShuttle;


/** An enumerable calculation containing physical-to-logical field conversions. */
public final class EnumerableMappingConvert extends EnumerableCalc {

    private static final double CPU_PER_CONVERSION = 10;
    private final List<RexCall> conversions;


    EnumerableMappingConvert( AlgTraitSet traits, AlgNode input, RexProgram program ) {
        super( input.getCluster(), traits, input, program );
        conversions = program.getExprList().stream().filter( expression -> expression.isA( Kind.MAPPING_CONVERT ) )
                .map( RexCall.class::cast ).toList();
    }


    public static EnumerableMappingConvert create( PolyAlgArgs args, List<AlgNode> inputs, AlgCluster cluster ) {
        AlgNode input = sole( inputs );
        return new EnumerableMappingConvert( input.getTraitSet(), input, getProgramFromArgs( args, input, cluster.getRexBuilder() ) );
    }


    @Override
    public EnumerableCalc copy( AlgTraitSet traitSet, AlgNode child, RexProgram program ) {
        return new EnumerableMappingConvert( traitSet, child, program );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        var baseCost = super.computeSelfCost( planner, mq );
        return mq.getTupleCount( getInput() ).map( rows -> baseCost.plus( planner.getCostFactory().makeCost( 0, conversionCpu( rows ), 0 ) ) )
                .orElse( baseCost );
    }


    @Override
    protected void addPolyAlgMetadata( PolyAlgMetadata metadata, AlgMetadataQuery mq ) {
        var expandLocalRefs = new RexShuttle() {
            @Override
            public RexNode visitLocalRef( RexLocalRef ref ) {
                return program.expandLocalRef( ref );
            }
        };
        var inputFields = getInput().getTupleType().getFieldNames();
        for ( RexCall call : conversions ) {
            var conversion = (RexCall) call.accept( expandLocalRefs );
            metadata.addBadge( PolyAlgUtils.describeMappingConversion( conversion, inputFields ), "conversion", PolyAlgMetadata.BadgeLevel.INFO );
        }
        mq.getTupleCount( getInput() ).ifPresent( rows -> metadata.addMetadata( "mappingConversionCpu", "Mapping conversion CPU", conversionCpu( rows ), true ) );
    }


    private double conversionCpu( double inputRows ) {
        return inputRows * conversions.size() * CPU_PER_CONVERSION;
    }

}
