/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.tools;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.val;
import org.polypheny.db.plan.Context;
import org.polypheny.db.plan.Contexts;
import org.polypheny.db.plan.RelOptCluster;
import org.polypheny.db.plan.RelOptSchema;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.type.RelDataType;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.transaction.Statement;

public class RoutedRelBuilder extends RelBuilder{

    @Getter
    protected Map<Long, List<Long>> physicalPlacementsOfPartitions = new HashMap<>(); // partitionId, list<CatalogPlacementIds>

    public RoutedRelBuilder( Context context, RelOptCluster cluster, RelOptSchema relOptSchema ) {
        super( context, cluster, relOptSchema );
    }

    public static RoutedRelBuilder create( Statement statement, RelOptCluster cluster){
        return new RoutedRelBuilder(  Contexts.EMPTY_CONTEXT, cluster, statement.getTransaction().getCatalogReader()  );
    }

    public static RoutedRelBuilder createCopy( Statement statement, RelOptCluster cluster, RoutedRelBuilder builder){
        val newBuilder = RoutedRelBuilder.create( statement, cluster );
        newBuilder.getPhysicalPlacementsOfPartitions().putAll( ImmutableMap.copyOf( builder.getPhysicalPlacementsOfPartitions() ) );

        for ( int i = 0; i < builder.stackSize(); i++ ) {
            newBuilder.push( builder.peek( i ) );
        }
        return newBuilder;
    }


    public RoutedRelBuilder values( Iterable<? extends List<RexLiteral>> tupleList, RelDataType rowType ) {
      super.values( tupleList, rowType );
        return this;
    }

    public RoutedRelBuilder scan( Iterable<String> tableNames ){
        super.scan( tableNames );
        return this;
    }

    public RoutedRelBuilder push( RelNode node ) {
        super.push( node );
        return this;
    }

}
