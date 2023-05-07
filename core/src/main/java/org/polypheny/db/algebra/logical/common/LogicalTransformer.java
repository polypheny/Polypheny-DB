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

package org.polypheny.db.algebra.logical.common;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.lang.NotImplementedException;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.core.common.Transformer;
import org.polypheny.db.algebra.logical.document.LogicalDocumentProject;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.schema.trait.ModelTrait;


public class LogicalTransformer extends Transformer {


    /**
     * Subclass of {@link Transformer} not targeted at any particular engine or calling convention.
     */
    public LogicalTransformer( AlgOptCluster cluster, List<AlgNode> inputs, List<String> names, ModelTrait inTraitSet, ModelTrait outTraitSet, AlgDataType rowType, boolean isCrossModel ) {
        super(
                cluster,
                inputs,
                names,
                inputs.get( 0 ).getTraitSet(),
                inTraitSet,
                outTraitSet,
                rowType,
                isCrossModel );
    }


    private static List<AlgNode> adjustIfNeeded( List<AlgNode> inputs, ModelTrait inTraitSet, ModelTrait outTraitSet, AlgDataType rowType ) {
        AlgNode node = inputs.get( 0 );
        if ( outTraitSet == ModelTrait.DOCUMENT ) {
            node = handleDocument( inputs, inTraitSet, outTraitSet, rowType );
        } else if ( outTraitSet == ModelTrait.RELATIONAL ) {
            node = handleRelational( inputs, inTraitSet, outTraitSet, rowType );
        } else if ( outTraitSet == ModelTrait.GRAPH ) {
            node = handleGraph( inputs, inTraitSet, outTraitSet, rowType );
        }
        return List.of( node );
    }


    public static AlgNode create( List<AlgNode> inputs, ModelTrait inTraitSet, ModelTrait outTraitSet, AlgDataType rowType ) {

        return new LogicalTransformer( inputs.get( 0 ).getCluster(), inputs, null, inTraitSet, outTraitSet, rowType, false );
    }


    private static AlgNode handleRelational( List<AlgNode> inputs, ModelTrait inTraitSet, ModelTrait outTraitSet, AlgDataType rowType ) {
        AlgDataTypeFactory factory = AlgDataTypeFactory.DEFAULT;
        AlgOptCluster cluster = inputs.get( 0 ).getCluster();
        RexBuilder rexBuilder = cluster.getRexBuilder();

        AlgNode input = inputs.get( 0 );

        List<RexNode> fields = new ArrayList<>();
        int dataId = rowType.getField( DocumentType.DOCUMENT_DATA, true, false ).getIndex();
        for ( AlgDataTypeField field : rowType.getFieldList() ) {
            if ( input.getRowType().getFieldNames().contains( field.getName() ) ) {
                fields.add( rexBuilder.makeInputRef( new DocumentType(), input.getRowType().getField( field.getName(), false, false ).getIndex() ) );
            } else if ( field.getName().equals( DocumentType.DOCUMENT_DATA ) ) {
                List<RexNode> nodes = new ArrayList<>();
                nodes.add( rexBuilder.makeInputRef( field.getType(), dataId ) );
                rowType.getFieldNames().forEach( s -> nodes.add( rexBuilder.makeLiteral( s ) ) );
                fields.add( rexBuilder.makeCall( new DocumentType(), OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.REMOVE_NAMES ), nodes ) );
            } else {
                fields.add( rexBuilder.makeCall( new DocumentType(), OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.EXTRACT_NAME ), rexBuilder.makeInputRef( field.getType(), dataId ), rexBuilder.makeLiteral( field.getName() ) ) );
            }

        }
        LogicalDocumentProject project = LogicalDocumentProject.create( input, fields, rowType.getFieldNames() );
        return new LogicalTransformer( inputs.get( 0 ).getCluster(), List.of( project ), null, inTraitSet, outTraitSet, rowType, false );

    }


    private static AlgNode handleGraph( List<AlgNode> inputs, ModelTrait inTraitSet, ModelTrait outTraitSet, AlgDataType rowType ) {
        throw new NotImplementedException();
    }


    private static AlgNode handleDocument( List<AlgNode> inputs, ModelTrait inTraitSet, ModelTrait outTraitSet, AlgDataType rowType ) {
        if ( inputs.size() == 1 && inputs.get( 0 ).getRowType().getFieldCount() == rowType.getFieldCount() ) {
            return new LogicalTransformer( inputs.get( 0 ).getCluster(), inputs, null, inTraitSet, outTraitSet, rowType, false );
        }

        return inputs.get( 0 );

    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalTransformer( inputs.get( 0 ).getCluster(), inputs, names, inModelTrait, outModelTrait, rowType, isCrossModel );
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        AlgWriter writer = super.explainTerms( pw );
        int i = 0;
        for ( AlgNode input : getInputs() ) {
            writer.input( "input#" + i, input );
            i++;
        }
        return writer;
    }


}
