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

package org.polypheny.db.adapter.enumerable;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.BlockStatement;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MemberDeclaration;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.enumerable.RexToLixTranslator.InputGetterImpl;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgWriter;
import org.polypheny.db.algebra.core.Transformer;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.Catalog.NamespaceType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.runtime.PolyCollections.PolyList;
import org.polypheny.db.runtime.PolyCollections.PolyMap;
import org.polypheny.db.schema.ModelTrait;
import org.polypheny.db.schema.ModelTraitDef;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Pair;

@Getter
public class EnumerableTransformer extends Transformer implements EnumerableAlg {

    private static final Map<PolyType, Pair<PolyType, Method>> DOCUMENT_TO_RELATIONAL_MAPPING;


    static {
        Builder<PolyType, Pair<PolyType, Method>> docToRelMap = ImmutableMap.builder();
        docToRelMap.put( PolyType.MAP, Pair.of( PolyType.JSON, BuiltInMethod.DOC_JSONIZE.method ) );
        docToRelMap.put( PolyType.DOCUMENT, Pair.of( PolyType.JSON, BuiltInMethod.DOC_JSONIZE.method ) );
        DOCUMENT_TO_RELATIONAL_MAPPING = docToRelMap.build();
    }


    // transformation is used to map from one schema to another
    private final Map<PolyType, Pair<PolyType, Method>> transformLookupMap;
    private final ModelTrait fromTrait;
    private final ModelTrait toTrait;

    // substitution is used to allow storing of unsupported types via serialization in stores
    private final Map<PolyType, Pair<Method, Class<?>>> substitutionLookupMap;
    private final Method substitutionMethod;
    private String substitutionDesc;
    private String mappingDesc;


    /**
     * Creates a <code>SingleRel</code>.
     *
     * @param cluster Cluster this relational expression belongs to
     * @param traits Set of traits, which this transformer produces,
     * these either are the same as its input (substitution) or a transformed set (transformation)
     * @param input Input relational expression
     */
    protected EnumerableTransformer(
            AlgOptCluster cluster,
            AlgTraitSet traits,
            AlgDataType rowType,
            AlgNode input,
            List<PolyType> unsupportedTypes,
            @Nullable PolyType substituteType, // null defines no substitution needed
            ModelTrait fromTrait,
            ModelTrait toTrait ) {
        super( cluster, traits, rowType, input, unsupportedTypes, substituteType );
        this.fromTrait = fromTrait;
        this.toTrait = toTrait;

        if ( fromTrait != toTrait ) {
            this.transformLookupMap = getMapping( fromTrait.getDataModel(), toTrait.getDataModel() );
        } else {
            this.transformLookupMap = ImmutableMap.of();
        }

        if ( substituteType == null ) {
            this.substitutionMethod = null;
            this.substitutionLookupMap = ImmutableMap.of();
            return;
        }

        this.substitutionMethod = getSubstitutionsMethod( substituteType );

        // define for a serialized original PolyType, which method with the appropriate end class has to be called
        Builder<PolyType, Pair<Method, Class<?>>> substitutionBuilder = ImmutableMap.builder();
        for ( PolyType unsupportedType : unsupportedTypes ) {
            substitutionBuilder.put( unsupportedType, Pair.of( substitutionMethod, getSubstitutionClass( unsupportedType ) ) );
        }
        this.substitutionLookupMap = substitutionBuilder.build();

        // description
        if ( !substitutionLookupMap.isEmpty() ) {
            this.substitutionDesc = String.format(
                    "[%s]->%s",
                    unsupportedTypes.stream().map( Enum::name ).collect( Collectors.joining( "," ) ), substituteType.name() );
        }
    }


    private Map<PolyType, Pair<PolyType, Method>> getMapping( NamespaceType from, NamespaceType to ) {

        if ( from == NamespaceType.DOCUMENT && to == NamespaceType.RELATIONAL ) {
            this.mappingDesc = "DOCUMENT_TO_RELATIONAL";
            return DOCUMENT_TO_RELATIONAL_MAPPING;
        }
        throw new UnsupportedOperationException( "The mapping from the data model: " + from + " to: " + to + " is not yet supported." );
    }


    private static Method getSubstitutionsMethod( PolyType substituteType ) {
        switch ( substituteType ) {
            case VARCHAR:
                return BuiltInMethod.DESERIALIZE_DECOMPRESS_STRING.method;
            case BINARY:
                return BuiltInMethod.DESERIALIZE_DECOMPRESS_BYTE_ARRAY.method;
            default:
                throw new UnsupportedOperationException( "For the substituted type no deserialize method was provided." );
        }
    }


    public static EnumerableTransformer create(
            Transformer transformer,
            AlgTraitSet traits,
            AlgNode input ) {

        return new EnumerableTransformer(
                transformer.getCluster(),
                traits,
                transformer.getRowType(),
                input,
                transformer.getUnsupportedTypes(),
                transformer.getSubstituteType(),
                transformer.getInput().getTraitSet().getTrait( ModelTraitDef.INSTANCE ),
                transformer.getTraitSet().getTrait( ModelTraitDef.INSTANCE ) );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        final BlockBuilder builder = new BlockBuilder();
        Result orig = implementor.visitChild( this, 0, (EnumerableAlg) input, pref );

        Expression childExp = builder.append( builder.newName( "child_" + System.nanoTime() ), orig.block );

        final PhysType physType = PhysTypeImpl.of( implementor.getTypeFactory(), getRowType(), pref.prefer( orig.format ) );
        Type inputJavaType = orig.physType.getJavaRowType();
        ParameterExpression inputEnumerator = Expressions.parameter( Types.of( Enumerator.class, inputJavaType ), "inputEnumerator" );

        Type outputJavaType = physType.getJavaRowType();
        final Type enumeratorType = Types.of( Enumerator.class, outputJavaType );

        Expression input = RexToLixTranslator.convert( Expressions.call( inputEnumerator, BuiltInMethod.ENUMERATOR_CURRENT.method ), inputJavaType );

        List<Integer> replacedIndexes = getReplacedFields( getRowType() );

        if ( replacedIndexes.isEmpty() && transformLookupMap.isEmpty() ) {
            // we need no transformation of fields and can directly return the child
            return orig;
        }

        InputGetterImpl getter = new InputGetterImpl( Collections.singletonList( Pair.of( input, orig.physType ) ) );

        final BlockBuilder builder2 = new BlockBuilder();

        // transform the rows with the substituted entries back to the needed format
        List<Expression> expressions = new ArrayList<>();
        for ( AlgDataTypeField field : getRowType().getFieldList() ) {

            Expression exp = getter.field( builder2, field.getIndex(), null );

            // do we need to transform from the serialized form
            if ( substitutionLookupMap.containsKey( field.getType().getPolyType() ) ) {
                Pair<Method, Class<?>> subInfo = substitutionLookupMap.get( field.getType().getPolyType() );
                exp = Expressions.call( subInfo.left, exp, Expressions.constant( subInfo.right ) );
            }

            // do we have to handle the transformation between models special
            if ( transformLookupMap.containsKey( field.getType().getPolyType() ) ) {
                exp = Expressions.call( transformLookupMap.get( field.getType().getPolyType() ).right, exp );
            }

            expressions.add( exp );
        }

        builder2.add( Expressions.return_( null, physType.record( expressions ) ) );
        BlockStatement currentBody = builder2.toBlock();

        Expression body = Expressions.new_(
                enumeratorType,
                EnumUtils.NO_EXPRS,
                Expressions.list(
                        Expressions.fieldDecl(
                                Modifier.PUBLIC | Modifier.FINAL,
                                inputEnumerator,
                                Expressions.call( childExp, BuiltInMethod.ENUMERABLE_ENUMERATOR.method ) ),
                        EnumUtils.overridingMethodDecl(
                                BuiltInMethod.ENUMERATOR_RESET.method,
                                EnumUtils.NO_PARAMS,
                                Blocks.toFunctionBlock( Expressions.call( inputEnumerator, BuiltInMethod.ENUMERATOR_RESET.method ) ) ),
                        EnumUtils.overridingMethodDecl(
                                BuiltInMethod.ENUMERATOR_MOVE_NEXT.method,
                                EnumUtils.NO_PARAMS,
                                Blocks.toFunctionBlock( Expressions.call( inputEnumerator, BuiltInMethod.ENUMERATOR_MOVE_NEXT.method ) ) ),
                        EnumUtils.overridingMethodDecl(
                                BuiltInMethod.ENUMERATOR_CLOSE.method,
                                EnumUtils.NO_PARAMS,
                                Blocks.toFunctionBlock( Expressions.call( inputEnumerator, BuiltInMethod.ENUMERATOR_CLOSE.method ) ) ),
                        Expressions.methodDecl(
                                Modifier.PUBLIC,
                                EnumUtils.BRIDGE_METHODS
                                        ? Object.class
                                        : outputJavaType,
                                "current",
                                EnumUtils.NO_PARAMS,
                                currentBody ) ) );

        builder.add(
                Expressions.return_(
                        null,
                        Expressions.new_(
                                BuiltInMethod.ABSTRACT_ENUMERABLE_CTOR.constructor,
                                // TODO: generics
                                //   Collections.singletonList(inputRowType),
                                EnumUtils.NO_EXPRS,
                                ImmutableList.<MemberDeclaration>of( Expressions.methodDecl( Modifier.PUBLIC, enumeratorType, BuiltInMethod.ENUMERABLE_ENUMERATOR.method.getName(), EnumUtils.NO_PARAMS, Blocks.toFunctionBlock( body ) ) ) ) ) );
        return implementor.result( physType, builder.toBlock() );
    }


    // maybe move to PolyType or RexLiteral
    private static Class<?> getSubstitutionClass( PolyType type ) {
        switch ( type ) {
            case MAP:
            case DOCUMENT:
                return PolyMap.class;
            case ARRAY:
                return PolyList.class;
            default:
                throw new RuntimeException( "The provided type is not supported for substitution." );
        }
    }


    @Override
    public AlgWriter explainTerms( AlgWriter pw ) {
        return super.explainTerms( pw ).item( "transformation", getTransformationDesc() );
    }


    private String getTransformationDesc() {
        List<String> descriptions = new ArrayList<>();
        if ( mappingDesc != null ) {
            descriptions.add( mappingDesc );
        }
        if ( substitutionDesc != null ) {
            descriptions.add( substitutionDesc );
        }

        return String.join( " & ", descriptions );
    }


    @Override
    public EnumerableTransformer copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new EnumerableTransformer( inputs.get( 0 ).getCluster(), traitSet, getRowType(), inputs.get( 0 ), getUnsupportedTypes(), getSubstituteType(), fromTrait, toTrait );
    }

}
