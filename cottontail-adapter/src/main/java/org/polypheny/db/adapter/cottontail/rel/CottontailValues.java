/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.adapter.cottontail.rel;


import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc.Data;
import ch.unibas.dmi.dbis.cottontail.grpc.CottontailGrpc.Tuple;
import com.google.common.collect.ImmutableList;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.NewExpression;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.commons.lang3.reflect.TypeUtils;
import org.polypheny.db.adapter.cottontail.util.CottontailTypeUtil;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelTraitSet;
import org.polypheny.db.rel.core.Values;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rel.type.RelDataTypeField;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.util.Pair;


public class CottontailValues extends Values implements org.polypheny.db.adapter.cottontail.rel.CottontailRel {


    public static final Type DATA_MAP_TYPE = TypeUtils.parameterize( HashMap.class, String.class, Data.class );
    public static final Type LIST_DATA_MAPS_TYPE = TypeUtils.parameterize( ArrayList.class, DATA_MAP_TYPE );

//    private static final Method MAP_PUT_METHOD = Types.lookupMethod( Map.class, "put",  )

    protected CottontailValues( RelOptCluster cluster, RelDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples, RelTraitSet traits ) {
        super( cluster, rowType, tuples, traits );
    }


    @Override
    public void implement( CottontailImplementContext context ) {

        BlockBuilder builder = context.blockBuilder;

        final ParameterExpression valuesMapList_ = Expressions.parameter( Modifier.FINAL, LIST_DATA_MAPS_TYPE, builder.newName( "valuesMapList" ) );
        final NewExpression valuesMapListCreator = Expressions.new_( LIST_DATA_MAPS_TYPE );
        builder = builder.append( Expressions.assign( valuesMapList_, valuesMapListCreator ) );



        final List<String> physicalColumnNames = new ArrayList<>();
        for ( RelDataTypeField field : this.rowType.getFieldList() ) {
            physicalColumnNames.add( context.cottontailTable.getPhysicalColumnName( field.getName() ) );
        }

        List<Tuple> tupleList = new ArrayList<>();

        for ( List<RexLiteral> tuple : tuples ) {
            Map<String, Data> singleTuple = new HashMap<>();
            final ParameterExpression valuesMap_ = Expressions.parameter( Modifier.FINAL, DATA_MAP_TYPE, builder.newName( "valuesMap" ) );
            final NewExpression valuesMapCreator = Expressions.new_( DATA_MAP_TYPE );
            builder = builder.append( Expressions.assign( valuesMap_, valuesMapCreator ) );

            for ( Pair<String, RexLiteral> pair : Pair.zip( physicalColumnNames, tuple ) ) {
                builder.append( null
//                        Expressions.call(  )
                );
                singleTuple.put( pair.left, CottontailTypeUtil.rexLiteralToData( pair.right ) );
            }

            tupleList.add( Tuple.newBuilder().putAllData( singleTuple ).build() );
        }

        context.values = tupleList;
    }
}
