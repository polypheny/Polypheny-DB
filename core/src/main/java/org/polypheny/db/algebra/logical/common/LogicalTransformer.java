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

package org.polypheny.db.algebra.logical.common;

import java.util.List;
import lombok.Setter;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.core.common.Transformer;
import org.polypheny.db.algebra.enumerable.EnumerableConvention;
import org.polypheny.db.algebra.polyalg.PolyAlgDeclaration.ParamType;
import org.polypheny.db.algebra.polyalg.arguments.EnumArg;
import org.polypheny.db.algebra.polyalg.arguments.ListArg;
import org.polypheny.db.algebra.polyalg.arguments.PolyAlgArgs;
import org.polypheny.db.algebra.polyalg.arguments.StringArg;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.DocumentType;
import org.polypheny.db.algebra.type.GraphType;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.schema.trait.ModelTrait;
import org.polypheny.db.schema.trait.ModelTraitDef;


@Setter
public class LogicalTransformer extends Transformer {


    /**
     * Subclass of {@link Transformer} not targeted at any particular engine or calling convention.
     */
    public LogicalTransformer( AlgCluster cluster, AlgTraitSet traitSet, List<AlgNode> inputs, List<String> names, ModelTrait inTraitSet, ModelTrait outTraitSet, AlgDataType rowType, boolean isCrossModel ) {
        super(
                cluster,
                inputs,
                names,
                traitSet.replace( EnumerableConvention.NONE ),
                inTraitSet,
                outTraitSet,
                rowType,
                isCrossModel );
    }


    public static AlgNode create( List<AlgNode> inputs, ModelTrait inTraitSet, ModelTrait outTraitSet, AlgDataType rowType ) {
        return LogicalTransformer.create( inputs.get( 0 ).getCluster(), inputs, null, inTraitSet, outTraitSet, rowType, false );
    }


    public static LogicalTransformer create( AlgCluster cluster, List<AlgNode> inputs, List<String> names, ModelTrait inTraitSet, ModelTrait outTraitSet, AlgDataType rowType, boolean isCrossModel ) {
        return new LogicalTransformer( cluster, inputs.get( 0 ).getTraitSet().replace( AlgCollations.EMPTY ), inputs, names, inTraitSet, outTraitSet, rowType, isCrossModel );
    }


    public static LogicalTransformer create( AlgCluster cluster, List<AlgNode> inputs, List<String> names, ModelTrait outModelTrait ) {
        ModelTrait inModelTrait = inputs.get( 0 ).getTraitSet().getTrait( ModelTraitDef.INSTANCE );
        if ( inModelTrait == null ) {
            System.out.println( "Default inModelTrait is used." );
            inModelTrait = ModelTrait.RELATIONAL;
        }
        System.out.println( "parsed inModelTrait: " + inModelTrait );
        AlgDataType type = switch ( outModelTrait.dataModel() ) {
            case DOCUMENT -> DocumentType.ofDoc(); // TODO: verify any -> doc works
            case GRAPH -> GraphType.of();
            case RELATIONAL -> switch ( inModelTrait.dataModel() ) {
                case DOCUMENT -> DocumentType.ofCrossRelational(); // TODO: verify doc -> rel works
                case GRAPH -> GraphType.ofRelational();
                case RELATIONAL -> inputs.get( 0 ).getTupleType();  // TODO: verify rel -> rel works
            };

        };
        System.out.println( "parsed rowtype: " + type );

        return create( cluster, inputs, names, inModelTrait, outModelTrait, type, !inModelTrait.satisfies( outModelTrait ) );
    }


    public static LogicalTransformer create( PolyAlgArgs args, List<AlgNode> children, AlgCluster cluster ) {
        List<String> names = args.getListArg( "names", StringArg.class ).map( StringArg::getArg );
        EnumArg<DataModel> out = args.getEnumArg( "outModel", DataModel.class );
        return create( cluster, children, names, out.getArg().getModelTrait() );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new LogicalTransformer( getCluster(), traitSet, inputs, names, inModelTrait, outModelTrait, rowType, isCrossModel );
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
    public PolyAlgArgs collectAttributes() {
        System.out.println( "traitset: " + traitSet );
        System.out.println( "names: " + names );
        System.out.println( "inTrait: " + inModelTrait );
        System.out.println( "outTrait: " + outModelTrait );
        System.out.println( "rowType: " + rowType );
        System.out.println( "isCrossModel: " + isCrossModel );

        PolyAlgArgs args = new PolyAlgArgs( getPolyAlgDeclaration() );
        args.put( "outModel", new EnumArg<>( outModelTrait.dataModel(), ParamType.DATAMODEL_ENUM ) );
        if ( names != null ) {
            args.put( "names", new ListArg<>( names, StringArg::new ) );
        }
        return args;
    }


}
