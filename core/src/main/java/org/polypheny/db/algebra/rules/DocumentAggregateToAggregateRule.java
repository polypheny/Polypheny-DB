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

package org.polypheny.db.algebra.rules;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.enumerable.EnumerableProject;
import org.polypheny.db.algebra.logical.document.LogicalDocumentAggregate;
import org.polypheny.db.algebra.logical.document.LogicalDocumentProject;
import org.polypheny.db.algebra.logical.relational.LogicalProject;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.plan.AlgOptRule;
import org.polypheny.db.plan.AlgOptRuleCall;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.AlgBuilder.GroupKey;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.util.ImmutableBitSet;

public class DocumentAggregateToAggregateRule extends AlgOptRule {

    public static final DocumentAggregateToAggregateRule INSTANCE = new DocumentAggregateToAggregateRule();


    public DocumentAggregateToAggregateRule() {
        super( operand( LogicalDocumentAggregate.class, any() ), "DOCUMENT_AGGREGATE_TO_AGGREGATE" );
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
        //nodes.add( parent );
        //names.add( alg.getInput().getRowType().getFieldNames().get( 0 ) );

        for ( String path : alg.groupSet ) {
            RexNode node = builder.getRexBuilder().makeCall(
                    DocumentType.ofId(),
                    OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.MQL_QUERY_VALUE ),
                    parent,
                    builder.getRexBuilder().makeArray( builder.getTypeFactory().createArrayType( builder.getTypeFactory().createPolyType( PolyType.CHAR, 255 ), -1 ),
                            PolyList.copyOf( Arrays.stream( path.split( "\\." ) ).map( PolyString::of ).collect( Collectors.toList() ) ) ) );
            nodes.add( node );
            names.add( path );
        }

        int i = 0;
        for ( String path : alg.names ) {

            RexNode node = builder.getRexBuilder().makeCall(
                    alg.aggCalls.get( i ).getType(),
                    OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.MQL_QUERY_VALUE ),
                    parent,
                    builder.getRexBuilder().makeArray( builder.getTypeFactory().createArrayType( builder.getTypeFactory().createPolyType( PolyType.CHAR, 255 ), -1 ),
                            PolyList.copyOf( Arrays.stream( path.split( "\\." ) ).map( PolyString::of ).collect( Collectors.toList() ) ) ) );

            if ( node.getType().getPolyType() != PolyType.DOCUMENT ) {
                node = builder.getRexBuilder().makeAbstractCast( node.getType(), node );
            }

            nodes.add( node );
            names.add( path );
            i++;
        }

        LogicalProject project = (LogicalProject) LogicalProject.create( alg.getInput(), nodes, names ).copy( alg.getInput().getTraitSet().replace( ModelTrait.DOCUMENT ), alg.getInputs() );

        EnumerableProject enumerableProject = new EnumerableProject( project.getCluster(), alg.getInput().getTraitSet().replace( ModelTrait.DOCUMENT ).replace( EnumerableConvention.INSTANCE ), convert( project.getInput(), EnumerableConvention.INSTANCE ), project.getProjects(), project.getRowType() );

        builder.push( enumerableProject );

        GroupKey groupKey = builder.groupKey( ImmutableBitSet.of( IntStream.range( 0, alg.groupSet.size() ).boxed().collect( Collectors.toList() ) ) );

        builder.aggregate( groupKey, alg.aggCalls );

        AlgNode aggregate = builder.build();

        AlgNode enumerableAggregate = aggregate.copy( aggregate.getTraitSet().replace( ModelTrait.DOCUMENT ), aggregate.getInputs() );

        //RexNode doc = builder.getRexBuilder().makeCall( DocumentType.ofId(), OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.MQL_MERGE ) );
        Map<String, RexNode> docs = enumerableAggregate.getRowType().getFieldList().stream().collect( Collectors.toMap( AlgDataTypeField::getName, e -> builder.getRexBuilder().makeInputRef( enumerableAggregate.getRowType(), e.getIndex() ) ) );

        call.transformTo( LogicalDocumentProject.create( enumerableAggregate, docs, List.of() ) );
        // call.transformTo( LogicalAggregate.create( alg.getInput(), alg.groupSet, alg.groupSets, alg.aggCalls ) );
    }

}
