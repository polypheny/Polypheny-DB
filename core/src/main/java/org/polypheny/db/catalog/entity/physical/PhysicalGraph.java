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

package org.polypheny.db.catalog.entity.physical;

import io.activej.serializer.annotations.Deserialize;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Value;
import lombok.experimental.NonFinal;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.type.entity.PolyValue;

@EqualsAndHashCode(callSuper = true)
@Value
@NonFinal
public class PhysicalGraph extends PhysicalEntity {

    public PhysicalGraph(
            @Deserialize("id") long id,
            @Deserialize("allocationId") long allocationId,
            @Deserialize("logicalId") long logicalId,
            @Deserialize("name") String name,
            @Deserialize("adapterId") long adapterId ) {
        super( id, allocationId, logicalId, name, id, name, List.of(), DataModel.GRAPH, adapterId ); // for graph both name and namespaceName are the same
    }


    @Override
    public PolyValue[] getParameterArray() {
        return new PolyValue[0];
    }


    @Override
    public Expression asExpression() {
        return Expressions.call( Catalog.CATALOG_EXPRESSION, "getPhysicalGraph", Expressions.constant( id ) );
    }


    @Override
    public PhysicalEntity normalize() {
        return new PhysicalGraph( id, allocationId, logicalId, name, adapterId );
    }

}
