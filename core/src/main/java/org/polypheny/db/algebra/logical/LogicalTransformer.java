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

package org.polypheny.db.algebra.logical;

import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.polypheny.db.algebra.AlgCollationTraitDef;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.Transformer;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.Convention;
import org.polypheny.db.schema.ModelTrait;
import org.polypheny.db.type.PolyType;

public class LogicalTransformer extends Transformer {


    public static AlgNode createDocumentToRelational( AlgNode node ) {
        /*AlgDataTypeFactory factory = node.getCluster().getTypeFactory();
        AlgRecordType substitutedRowType = new AlgRecordType( node.getRowType().getFieldList().stream().map( f -> {
            if ( f.getType().getPolyType() == PolyType.DOCUMENT ) {
                return new AlgDataTypeFieldImpl( f.getName(), f.getPhysicalName(), f.getIndex(), factory.createPolyType( PolyType.VARCHAR, 2024 ) );
            }
            return f;
        } ).collect( Collectors.toList() ) );*/

        return new LogicalTransformer( node.getCluster(), node.getTraitSet().replace( ModelTrait.RELATIONAL ), node, node.getRowType(), Collections.emptyList(), null );

    }


    public static LogicalTransformer merge( LogicalTransformer parent, LogicalTransformer child ) {
        if ( parent.getSubstituteType() != child.getSubstituteType() && parent.getSubstituteType() != null ) {
            // we do not have compatible transformations and no transformation -> substitution
            return null;
        }
        return new LogicalTransformer(
                parent.getCluster(),
                parent.getTraitSet(),
                child.input,
                parent.getRowType(),
                Stream.concat( parent.getUnsupportedTypes().stream(), child.getUnsupportedTypes().stream() ).collect( Collectors.toList() ),
                child.getSubstituteType() );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalTransformer( inputs.get( 0 ).getCluster(), traitSet, inputs.get( 0 ), rowType, getUnsupportedTypes(), getSubstituteType() );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        return input.computeSelfCost( planner, mq );
    }


    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits
     * @param original Input relational expression
     * @param rowType
     * @param unsupportedTypes
     * @param substituteType
     */
    protected LogicalTransformer( AlgOptCluster cluster, AlgTraitSet traits, AlgNode original, AlgDataType rowType, List<PolyType> unsupportedTypes, PolyType substituteType ) {
        super( cluster, traits, rowType, original, unsupportedTypes, substituteType );
    }


    public static LogicalTransformer create( AlgNode input, AlgDataType rowType, List<PolyType> unsupportedTypes, PolyType substituteType ) {

        final AlgTraitSet traitSet =
                input.getCluster().traitSetOf( Convention.NONE )
                        .replaceIfs(
                                AlgCollationTraitDef.INSTANCE,
                                ImmutableList::of );
        // add trait switch here
        return new LogicalTransformer( input.getCluster(), traitSet, input, rowType, unsupportedTypes, substituteType );
    }

}
