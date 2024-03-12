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

package org.polypheny.db.adapter.mongodb.rules;


import com.google.common.collect.Lists;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.polypheny.db.adapter.mongodb.MongoAlg.Implementor;
import org.polypheny.db.adapter.mongodb.MongoMethod;
import org.polypheny.db.adapter.mongodb.util.MongoTupleType;
import org.polypheny.db.algebra.AbstractAlgNode;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.convert.ConverterImpl;
import org.polypheny.db.algebra.core.common.Modify.Operation;
import org.polypheny.db.algebra.enumerable.EnumerableAlg;
import org.polypheny.db.algebra.enumerable.EnumerableAlgImplementor;
import org.polypheny.db.algebra.enumerable.JavaTupleFormat;
import org.polypheny.db.algebra.enumerable.PhysType;
import org.polypheny.db.algebra.enumerable.PhysTypeImpl;
import org.polypheny.db.algebra.metadata.AlgMetadataQuery;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgOptCost;
import org.polypheny.db.plan.AlgPlanner;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.plan.ConventionTraitDef;
import org.polypheny.db.runtime.Hook;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Pair;


/**
 * Relational expression representing a relScan of a table in a Mongo data source.
 */
@Slf4j
public class MongoToEnumerableConverter extends ConverterImpl implements EnumerableAlg {

    protected MongoToEnumerableConverter( AlgCluster cluster, AlgTraitSet traits, AlgNode input ) {
        super( cluster, ConventionTraitDef.INSTANCE, traits, input );
    }


    @Override
    public AlgNode copy( AlgTraitSet traitSet, List<AlgNode> inputs ) {
        return new MongoToEnumerableConverter( getCluster(), traitSet, AbstractAlgNode.sole( inputs ) );
    }


    @Override
    public AlgOptCost computeSelfCost( AlgPlanner planner, AlgMetadataQuery mq ) {
        return super.computeSelfCost( planner, mq ).multiplyBy( .1 );
    }


    @Override
    public Result implement( EnumerableAlgImplementor implementor, Prefer pref ) {
        final BlockBuilder list = new BlockBuilder();
        final Implementor mongoImplementor = new Implementor();
        mongoImplementor.visitChild( 0, getInput() );

        final AlgDataType rowType = getTupleType();
        final PhysType physType = PhysTypeImpl.of( implementor.getTypeFactory(), rowType, pref.prefer( JavaTupleFormat.ARRAY ) );

        if ( mongoImplementor.getEntity() == null ) {
            return implementor.result( physType, new BlockBuilder().toBlock() );
        }

        final Expression tupleTypes = MongoTupleType.from( rowType ).asExpression();

        final Expression table = list.append( "table", mongoImplementor.getEntity().asExpression() );

        List<String> opList = Pair.right( mongoImplementor.list );

        final Expression ops = list.append( "ops", constantArrayList( opList, String.class ) );
        final Expression filter = list.append( "filter", Expressions.constant( mongoImplementor.getFilterSerialized() ) );

        Expression enumerable;
        if ( !mongoImplementor.isDML() ) {
            final Expression logicalCols = list.append(
                    "logical",
                    constantArrayList(
                            opList.isEmpty() ? mongoImplementor.reorderPhysical() : mongoImplementor.getNecessaryPhysicalFields(), String.class ) );
            final Expression preProjects = list.append( "prePro", constantArrayList( mongoImplementor.getPreProjects(), String.class ) );
            enumerable = list.append(
                    list.newName( "enumerable" ),
                    Expressions.call( table, MongoMethod.MONGO_QUERYABLE_AGGREGATE.method, tupleTypes, ops, preProjects, logicalCols ) );
        } else {
            final Expression operations = list.append( list.newName( "operations" ), constantArrayList( mongoImplementor.getOperations(), String.class ) );
            final Expression operation = list.append( list.newName( "operation" ), Expressions.constant( mongoImplementor.getOperation(), Operation.class ) );
            final Expression onlyOne = list.append( list.newName( "onlyOne" ), Expressions.constant( mongoImplementor.onlyOne, boolean.class ) );
            final Expression needsDocument = list.append( list.newName( "needsUpdate" ), Expressions.constant( mongoImplementor.isDocumentUpdate, boolean.class ) );
            enumerable = list.append(
                    list.newName( "enumerable" ),
                    Expressions.call( table, MongoMethod.HANDLE_DIRECT_DML.method, operation, filter, operations, onlyOne, needsDocument ) );
        }

        if ( RuntimeConfig.DEBUG.getBoolean() ) {
            log.info( "Mongo: {}", opList );
        }
        Hook.QUERY_PLAN.run( opList );
        list.add( Expressions.return_( null, enumerable ) );
        return implementor.result( physType, list.toBlock() );
    }


    /**
     * E.g. {@code constantArrayList("x", "y")} returns "Arrays.asList('x', 'y')".
     *
     * @param values List of values
     * @param clazz Type of values
     * @return expression
     */
    protected static <T> MethodCallExpression constantArrayList( List<T> values, Class clazz ) {
        return Expressions.call( BuiltInMethod.ARRAYS_AS_LIST.method, Expressions.newArrayInit( clazz, constantList( values ) ) );
    }


    /**
     * E.g. {@code constantList("x", "y")} returns {@code {ConstantExpression("x"), ConstantExpression("y")}}.
     */
    protected static <T> List<Expression> constantList( List<T> values ) {
        return Lists.transform( values, Expressions::constant );
    }

}

