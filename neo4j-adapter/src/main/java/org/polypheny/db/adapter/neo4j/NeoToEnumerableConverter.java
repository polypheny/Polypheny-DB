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

package org.polypheny.db.adapter.neo4j;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.enumerable.EnumUtils;
import org.polypheny.db.adapter.enumerable.EnumerableAlg;
import org.polypheny.db.adapter.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.adapter.enumerable.JavaRowFormat;
import org.polypheny.db.adapter.enumerable.PhysType;
import org.polypheny.db.adapter.enumerable.PhysTypeImpl;
import org.polypheny.db.adapter.neo4j.NeoGraph.NeoQueryable;
import org.polypheny.db.adapter.neo4j.util.NeoUtil;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterImpl;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.plan.AlgOptCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgOptPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.ConventionTraitDef;
import org.polypheny.db.schema.ModelTrait;
import org.polypheny.db.schema.SchemaPlus;
import org.polypheny.db.schema.Schemas;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Pair;


/**
 * {@link ConverterImpl}, which serves as a bridge between the enumerable algebra and the Neo4j specific algebra opertors.
 */
public class NeoToEnumerableConverter extends ConverterImpl implements EnumerableAlg {

    /**
     * Creates a ConverterImpl.
     *
     * @param cluster planner's cluster
     * @param traits the output traits of this converter
     * @param child child alg (provides input traits)
     */
    protected NeoToEnumerableConverter( AlgOptCluster cluster, AlgTraitSet traits, AlgNode child ) {
        super( cluster, ConventionTraitDef.INSTANCE, traits, child );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgOptPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( 0.8 );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        final BlockBuilder blockBuilder = new BlockBuilder();
        if ( this.getTraitSet().contains( ModelTrait.RELATIONAL ) ) {
            return getRelationalImplement( implementor, pref, blockBuilder );
        } else {
            return getGraphImplement( implementor, pref, blockBuilder );
        }

    }


    /**
     * This methods generates the code snippet, which is used for the graph model logic for the Neo4j adapter.
     *
     * @param implementor is used build the code snippets by recursively moving through them
     * @param pref preferred result format, e.g. when SCALAR -> single result gets returned as single element, if ARRAY it is wrapped in an array
     * @param blockBuilder helper builder to generate expressions
     * @return the code in a result representation
     */
    private Result getGraphImplement( EnumerableAlgImplementor implementor, Prefer pref, BlockBuilder blockBuilder ) {
        final NeoGraphImplementor graphImplementor = new NeoGraphImplementor();

        graphImplementor.visitChild( 0, getInput() );

        final AlgDataType rowType = getRowType();

        // PhysType is Enumerable Adapter class that maps database types (getRowType) with physical Java types (getJavaTypes())
        final PhysType physType = PhysTypeImpl.of( implementor.getTypeFactory(), rowType, pref.prefer( JavaRowFormat.ARRAY ) );

        final Expression graph = blockBuilder.append( "graph", Expressions.convert_(
                Expressions.call( Schemas.class, "graph", DataContext.ROOT,
                        Expressions.convert_(
                                Expressions.call(
                                        Expressions.call(
                                                DataContext.ROOT,
                                                BuiltInMethod.DATA_CONTEXT_GET_ROOT_SCHEMA.method ),
                                        BuiltInMethod.SCHEMA_GET_SUB_SCHEMA.method,
                                        Expressions.constant( graphImplementor.getGraph().name, String.class ) ), SchemaPlus.class ) ), NeoQueryable.class ) );

        Expression enumerable;
        if ( graphImplementor.isAll() && rowType.getFieldCount() == 1 && rowType.getFieldList().get( 0 ).getType().getPolyType() == PolyType.GRAPH ) {
            Pair<String, String> queries = graphImplementor.getAllQueries();

            enumerable = blockBuilder.append(
                    blockBuilder.newName( "enumerable" ),
                    Expressions.call(
                            graph,
                            NeoMethod.GRAPH_ALL.method, Expressions.constant( queries.left ), Expressions.constant( queries.right ) ) );

        } else {
            final Expression fields = getFields( blockBuilder, rowType, AlgDataType::getPolyType );

            final Expression arrayFields = getFields( blockBuilder, rowType, NeoUtil::getComponentTypeOrParent );

            final Expression parameterClasses = Expressions.constant( null );

            final String query = graphImplementor.build();

            enumerable = blockBuilder.append(
                    blockBuilder.newName( "enumerable" ),
                    Expressions.call(
                            graph,
                            NeoMethod.GRAPH_EXECUTE.method, Expressions.constant( query ), fields, arrayFields, parameterClasses ) );
        }

        blockBuilder.add( Expressions.return_( null, enumerable ) );

        return implementor.result( physType, blockBuilder.toBlock() );
    }


    /**
     * Generates the relational specific code representation of the attached child nodes.
     *
     * @param implementor is used build the code snippets by recursively moving through them
     * @param pref preferred result format, e.g. when SCALAR -> single result gets returned as single element, if ARRAY it is wrapped in an array
     * @param blockBuilder helper builder to generate expressions
     * @return the code in a result representation
     */
    private Result getRelationalImplement( EnumerableAlgImplementor implementor, Prefer pref, BlockBuilder blockBuilder ) {
        final NeoRelationalImplementor neoImplementor = new NeoRelationalImplementor();

        neoImplementor.visitChild( 0, getInput() );

        final AlgDataType rowType = getRowType();

        // PhysType is Enumerable Adapter class that maps SQL types (getRowType) with physical Java types (getJavaTypes())
        final PhysType physType = PhysTypeImpl.of( implementor.getTypeFactory(), rowType, pref.prefer( JavaRowFormat.ARRAY ) );

        final Expression table = blockBuilder.append( "table", neoImplementor.getTable().getExpression( NeoEntity.NeoQueryable.class ) );

        final Expression fields = getFields( blockBuilder, rowType, AlgDataType::getPolyType );

        final Expression arrayFields = getFields( blockBuilder, rowType, NeoUtil::getComponentTypeOrParent );

        final Expression parameterClasses = getPolyMap( blockBuilder, neoImplementor.getPreparedTypes() );

        final String query = neoImplementor.build();

        final Expression enumerable = blockBuilder.append(
                blockBuilder.newName( "enumerable" ),
                Expressions.call(
                        table,
                        NeoMethod.EXECUTE.method, Expressions.constant( query ), fields, arrayFields, parameterClasses ) );

        blockBuilder.add( Expressions.return_( null, enumerable ) );

        return implementor.result( physType, blockBuilder.toBlock() );
    }


    /**
     * Generates the {@link Expression} equivalent of a map with {@link PolyType} pairs as values
     *
     * @param map the target map to transform
     * @return the map in an expression representation
     */
    private Expression getPolyMap( BlockBuilder builder, Map<Long, Pair<PolyType, PolyType>> map ) {
        return builder.append(
                builder.newName( "map" ),
                Expressions.call(
                        BuiltInMethod.MAP_OF_ENTRIES.method,
                        EnumUtils.expressionList(
                                map.entrySet()
                                        .stream()
                                        .map( p ->
                                                Expressions.call( BuiltInMethod.PAIR_OF.method, Expressions.constant( p.getKey(), Long.class ),
                                                        getPair( p.getValue(), PolyType.class, PolyType.class ) ) ).collect( Collectors.toList() ) ) ) );
    }


    /**
     * Returns the provided pair as {@link Expression}.
     *
     * @param pair the target pair
     * @param classLeft class of the left element
     * @param classRight class of the right element
     * @return the pair as Expression e.g. <code>Pair.of( (int) 0, (long) 3 )</code>
     */
    public <T, E> Expression getPair( Pair<T, E> pair, Class<T> classLeft, Class<E> classRight ) {
        return Expressions.call( BuiltInMethod.PAIR_OF.method, Expressions.constant( pair.left, classLeft ), Expressions.constant( pair.left, classRight ) );
    }


    /**
     * Returns the {@link Expression} for the given {@link AlgDataType} where each field is represented as {@link PolyType}.
     *
     * @param builder helper builder to generate expressions
     * @param rowType the RowType to transform
     * @param typeGetter function, which transforms a {@link AlgDataType} into a matching {@link PolyType}
     */
    public Expression getFields( BlockBuilder builder, AlgDataType rowType, Function1<AlgDataType, PolyType> typeGetter ) {
        return builder.append(
                builder.newName( "fields" ),
                EnumUtils.constantArrayList( rowType
                        .getFieldList()
                        .stream()
                        .map( f -> typeGetter.apply( f.getType() ) )
                        .collect( Collectors.toList() ), PolyType.class ) );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new NeoToEnumerableConverter( inputs.get( 0 ).getCluster(), traitSet, inputs.get( 0 ) );
    }

}
