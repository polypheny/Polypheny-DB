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
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.core.LaxAggregateCall;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.enumerable.EnumerableProject;
import org.polypheny.db.algebra.logical.document.LogicalDocumentAggregate;
import org.polypheny.db.algebra.logical.document.LogicalDocumentProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelAggregate;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNameRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.ImmutableBitSet;

public class DocumentAggregateToAggregateRule extends AlgOptRule {

    public static final DocumentAggregateToAggregateRule INSTANCE = new DocumentAggregateToAggregateRule();


    public DocumentAggregateToAggregateRule() {
        super( operand( LogicalDocumentAggregate.class, any() ), DocumentAggregateToAggregateRule.class.getSimpleName() );
    }


    /**
     * Replaces a doc aggregate with the project + aggregate + project
     *
     * @param call Rule call
     */
    @Override
    public void onMatch( AlgOptRuleCall call ) {
        LogicalDocumentAggregate alg = call.alg( 0 );

        AlgBuilder builder = call.builder();
        List<RexNode> nodes = new ArrayList<>();
        List<String> names = new ArrayList<>();
        RexIndexRef parent = builder.getRexBuilder().makeInputRef( alg.getInput(), 0 );

        ImmutableBitSet groupSet = ImmutableBitSet.of();
        if ( alg.getGroup().isPresent() ) {
            groupSet = ImmutableBitSet.of( List.of( 0 ) );
            String groupKey = alg.getGroup().get().getName();
            RexNode node = builder.getRexBuilder().makeCall(
                    DocumentType.ofId(),
                    OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.MQL_QUERY_VALUE ),
                    parent,
                    builder.getRexBuilder().makeArray( builder.getTypeFactory().createArrayType( builder.getTypeFactory().createPolyType( PolyType.CHAR, 255 ), -1 ),
                            PolyList.copyOf( Arrays.stream( groupKey.split( "\\." ) ).map( o -> (PolyValue) PolyString.of( o ) ).toList() ) ) );
            nodes.add( node );
            names.add( DocumentType.DOCUMENT_ID );
        }

        for ( LaxAggregateCall agg : alg.aggCalls ) {
            RexNode node = RexIndexRef.of( 0, DocumentType.ofDoc() );
            if ( agg.getInput().isPresent() ) {
                node = builder.getRexBuilder().makeCall(
                        DocumentType.ofDoc(),
                        OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.MQL_QUERY_VALUE ),
                        parent,
                        builder.getRexBuilder().makeArray( builder.getTypeFactory().createArrayType( builder.getTypeFactory().createPolyType( PolyType.CHAR, 255 ), -1 ),
                                PolyList.copyOf( agg.getInput().map( r -> r.unwrap( RexNameRef.class ).map( n -> n.names.stream().map( o -> (PolyValue) PolyString.of( o ) ) ).orElseThrow() ).orElseThrow().toList() ) ) );
            }

            if ( agg.requiresCast( alg.getCluster() ).isPresent() ) {
                node = builder.getRexBuilder().makeAbstractCast( agg.requiresCast( alg.getCluster() ).get(), node );
            }

            if ( node.getType().getPolyType() != PolyType.DOCUMENT ) {
                node = builder.getRexBuilder().makeAbstractCast( node.getType(), node );
            }

            nodes.add( node );
            names.add( agg.name );
        }

        LogicalRelProject project = (LogicalRelProject) LogicalRelProject.create( alg.getInput(), nodes, names ).copy( alg.getInput().getTraitSet().replace( ModelTrait.DOCUMENT ), alg.getInputs() );

        EnumerableProject enumerableProject = new EnumerableProject( project.getCluster(), alg.getInput().getTraitSet().replace( ModelTrait.DOCUMENT ).replace( EnumerableConvention.INSTANCE ), convert( project.getInput(), EnumerableConvention.INSTANCE ), project.getProjects(), project.getTupleType() );

        builder.push( enumerableProject );

        builder.push( LogicalRelAggregate.create( builder.build(), groupSet, null, alg.aggCalls.stream().map( a -> a.toAggCall( project.getTupleType(), alg.getCluster() ) ).toList() ) );

        AlgNode aggregate = builder.build();

        AlgNode enumerableAggregate = aggregate.copy( aggregate.getTraitSet().replace( ModelTrait.DOCUMENT ), aggregate.getInputs() );

        //RexNode doc = builder.getRexBuilder().makeCall( DocumentType.ofId(), OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.MQL_MERGE ) );
        Map<String, RexNode> docs = enumerableAggregate.getTupleType().getFields().stream().collect( Collectors.toMap( AlgDataTypeField::getName, e -> e.getName().equals( DocumentType.DOCUMENT_ID )
                ? builder.getRexBuilder().makeCall( DocumentType.ofDoc(), OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.MQL_NOT_UNSET ), builder.getRexBuilder().makeInputRef( DocumentType.ofDoc(), e.getIndex() ) )
                : builder.getRexBuilder().makeInputRef( DocumentType.ofDoc(), e.getIndex() ) ) );

        // RexNode doc = builder.getRexBuilder().makeCall( DocumentType.ofId(), OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.MQL_MERGE ), nodes );

        call.transformTo( LogicalDocumentProject.create( enumerableAggregate, docs, List.of() ) );
        // call.transformTo( LogicalAggregate.create( alg.getInput(), alg.groupSet, alg.groupSets, alg.aggCalls ) );*/
    }

}
