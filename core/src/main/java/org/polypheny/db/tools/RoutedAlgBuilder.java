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

package org.polypheny.db.tools;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.snapshot.Snapshot;
import org.polypheny.db.plan.AlgCluster;
import org.polypheny.db.plan.Context;
import org.polypheny.db.plan.Contexts;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.routing.FieldDistribution;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.type.entity.document.PolyDocument;


/**
 * Extension of AlgBuilder for routed plans with some more information.
 */
@Setter
@Getter
public class RoutedAlgBuilder extends AlgBuilder {

    protected FieldDistribution fieldDistribution; // PartitionId -> List<AllocationColumn>


    public RoutedAlgBuilder( Context context, AlgCluster cluster, Snapshot snapshot ) {
        super( context, cluster, snapshot );
    }


    public static RoutedAlgBuilder create( Statement statement, AlgCluster cluster ) {
        return new RoutedAlgBuilder( Contexts.EMPTY_CONTEXT, cluster, statement.getTransaction().getSnapshot() );
    }


    @Override
    public RoutedAlgBuilder values( Iterable<? extends List<RexLiteral>> tupleList, AlgDataType rowType ) {
        super.values( tupleList, rowType );
        return this;
    }


    @Override
    public RoutedAlgBuilder relScan( List<String> tableNames ) {
        super.relScan( tableNames );
        return this;
    }


    @Override
    public RoutedAlgBuilder push( AlgNode node ) {
        super.push( node );
        return this;
    }


    @Override
    public RoutedAlgBuilder documents( List<PolyDocument> documents, AlgDataType rowType ) {
        super.documents( documents, rowType );
        return this;
    }



}
