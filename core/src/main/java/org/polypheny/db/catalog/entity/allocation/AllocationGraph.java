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

package org.polypheny.db.catalog.entity.allocation;

import io.activej.serializer.annotations.Deserialize;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.experimental.SuperBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.catalog.logistic.DataModel;

@EqualsAndHashCode(callSuper = true)
@Value
@NonFinal
@SuperBuilder(toBuilder = true)
public class AllocationGraph extends AllocationEntity {


    public AllocationGraph(
            @Deserialize("id") long id,
            @Deserialize("placementId") long placementId,
            @Deserialize("partitionId") long partitionId,
            @Deserialize("logicalId") long logicalId,
            @Deserialize("namespaceId") long namespaceId,
            @Deserialize("adapterId") long adapterId ) {
        super( id, placementId, partitionId, logicalId, namespaceId, adapterId, DataModel.GRAPH );
    }


    @Override
    public Expression asExpression() {
        return Expressions.call( Catalog.CATALOG_EXPRESSION, "getAllocGraph", Expressions.constant( id ) );
    }


    @Override
    public Entity withName( String name ) {
        return toBuilder().name( name ).build();
    }

}
