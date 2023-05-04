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
import java.util.Map;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.commons.lang.NotImplementedException;
import org.polypheny.db.adapter.enumerable.EnumUtils;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttle;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.core.common.Transformer;
import org.polypheny.db.algebra.logical.relational.LogicalProject;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.runtime.functions.Functions;
import org.polypheny.db.schema.trait.ModelTrait;


public class LogicalTransformer extends Transformer {


    /**
     * Subclass of {@link Transformer} not targeted at any particular engine or calling convention.
     */
    public LogicalTransformer( AlgOptCluster cluster, List<AlgNode> inputs, List<String> names, AlgTraitSet traitSet, ModelTrait inTraitSet, ModelTrait outTraitSet, AlgDataType rowType, boolean isCrossModel ) {
        super( cluster, inputs, names, traitSet.replace( outTraitSet ), inTraitSet, outTraitSet, rowType, isCrossModel );
    }


    public static AlgNode create( List<AlgNode> inputs, AlgTraitSet traitSet, ModelTrait inTraitSet, ModelTrait outTraitSet, AlgDataType rowType ) {

        if ( outTraitSet == ModelTrait.DOCUMENT ) {
            return handleDocument( inputs, traitSet, inTraitSet, outTraitSet, rowType );
        } else if ( outTraitSet == ModelTrait.RELATIONAL ) {
            return handleRelational( inputs, traitSet, inTraitSet, outTraitSet, rowType );
        } else if ( outTraitSet == ModelTrait.GRAPH ) {
            return handleGraph( inputs, traitSet, inTraitSet, outTraitSet, rowType );
        }

        return new LogicalTransformer( inputs.get( 0 ).getCluster(), inputs, null, traitSet, inTraitSet, outTraitSet, rowType, false );
    }


    private static AlgNode handleRelational( List<AlgNode> inputs, AlgTraitSet traitSet, ModelTrait inTraitSet, ModelTrait outTraitSet, AlgDataType rowType ) {

        List<String> extract = new ArrayList<>();
        int i = -1;
        for ( AlgDataTypeField field : rowType.getFieldList() ) {
            if ( field.getName().equals( DocumentType.DOCUMENT_DATA ) ) {
                i = field.getIndex();
                continue;
            }
            extract.add( field.getName() );
        }

        LogicalProject project = LogicalProject.create( inputs.get( 0 ) );

        return new LogicalTransformer( inputs.get( 0 ).getCluster(), List.of( project ), null, traitSet, inTraitSet, outTraitSet, rowType, false );
    }


    private static AlgNode handleGraph( List<AlgNode> inputs, AlgTraitSet traitSet, ModelTrait inTraitSet, ModelTrait outTraitSet, AlgDataType rowType ) {
        throw new NotImplementedException();
    }


    private static AlgNode handleDocument( List<AlgNode> inputs, AlgTraitSet traitSet, ModelTrait inTraitSet, ModelTrait outTraitSet, AlgDataType rowType ) {
        if ( inputs.size() == 1 && inputs.get( 0 ).getRowType().getFieldCount() == rowType.getFieldCount() ) {
            return new LogicalTransformer( inputs.get( 0 ).getCluster(), inputs, null, traitSet, inTraitSet, outTraitSet, rowType, false );
        }

        List<Expression> expressions = new ArrayList<>();
        for ( AlgDataTypeField field : inputs.get( 0 ).getRowType().getFieldList() ) {
            if ( field.getName().equals( DocumentType.DOCUMENT_DATA ) ) {
                continue;
            }
            extract.add( field.getName() );
        }

        MethodCallExpression exp = Expressions.call( Functions.class, "extractNames", Expressions.convert_( Expressions.call( EnumUtils.class, "ofEntries", ), Map.class ), EnumUtils.constantArrayList( extract, String.class ) );

        LogicalProject.create()

    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalTransformer( inputs.get( 0 ).getCluster(), inputs, names, traitSet, inModelTrait, outModelTrait, rowType, isCrossModel );
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


    @Override
    public AlgNode accept( AlgShuttle shuttle ) {
        return shuttle.visit( this );
    }

}
