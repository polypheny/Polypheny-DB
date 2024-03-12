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

package org.polypheny.db.adapter.cottontail.algebra;

import com.google.common.collect.ImmutableList;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.NewExpression;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.cottontail.util.CottontailTypeUtil;
import org.polypheny.db.algebra.core.Values;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.AlgTraitSet;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Pair;


public class CottontailValues extends Values implements CottontailAlg {

    public CottontailValues( AlgCluster cluster, AlgDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples, AlgTraitSet traits ) {
        super( cluster, rowType, tuples, traits );
    }


    @Override
    public void implement( CottontailImplementContext context ) {
        BlockBuilder builder = context.blockBuilder;

        final ParameterExpression valuesMapList_ = Expressions.parameter( ArrayList.class, builder.newName( "valuesMapList" ) );
        final NewExpression valuesMapListCreator = Expressions.new_( ArrayList.class );
        builder.add( Expressions.declare( Modifier.FINAL, valuesMapList_, valuesMapListCreator ) );

        final List<Pair<String, PolyType>> physicalColumnNames = new ArrayList<>();
        List<Integer> tupleIndexes = new ArrayList<>();
        int i = 0;
        List<String> fieldNames = context.table.getTupleType().getFieldNames();
        for ( AlgDataTypeField field : this.rowType.getFields() ) {
            if ( !fieldNames.contains( field.getName() ) ) {
                continue;
            }
            int index = fieldNames.indexOf( field.getName() );
            physicalColumnNames.add( new Pair<>(
                    context.table.getTupleType().getFields().get( index ).getPhysicalName(),
                    field.getType().getPolyType() ) );

            tupleIndexes.add( field.getIndex() );
        }

        for ( List<RexLiteral> tuple : tuples ) {
            final ParameterExpression valuesMap_ = Expressions.variable( Map.class, builder.newName( "valuesMap" ) );
            final NewExpression valuesMapCreator = Expressions.new_( LinkedHashMap.class );
            builder.add( Expressions.declare( Modifier.FINAL, valuesMap_, valuesMapCreator ) );

            List<RexLiteral> values;
            if ( this.rowType.getFields().size() > physicalColumnNames.size() ) {
                values = new ArrayList<>();
                for ( int idx : tupleIndexes ) {
                    values.add( tuple.get( idx ) );
                }
            } else {
                values = tuple;
            }

            for ( Pair<Pair<String, PolyType>, RexLiteral> pair : Pair.zip( physicalColumnNames, values ) ) {
                builder.add(
                        Expressions.statement(
                                Expressions.call(
                                        valuesMap_,
                                        BuiltInMethod.MAP_PUT.method,
                                        Expressions.constant( pair.left.left ),
                                        CottontailTypeUtil.rexLiteralToDataExpression( pair.right, pair.left.right ) ) )
                );
            }

            builder.add( Expressions.statement( Expressions.call( valuesMapList_, Types.lookupMethod( List.class, "add", Object.class ), valuesMap_ ) ) );
        }

        context.blockBuilder = builder;
        context.valuesHashMapList = valuesMapList_;
    }

}
