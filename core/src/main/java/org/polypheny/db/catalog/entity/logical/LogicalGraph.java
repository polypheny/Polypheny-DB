/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.catalog.entity.logical;

import com.drew.lang.annotations.NotNull;
import io.activej.serializer.annotations.Deserialize;
import io.activej.serializer.annotations.Serialize;
import java.io.Serializable;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.catalog.logistic.NamespaceType;

@EqualsAndHashCode(callSuper = true)
@Value
public class LogicalGraph extends LogicalEntity implements Comparable<LogicalGraph> {

    private static final long serialVersionUID = 7343856827901459672L;

    @Serialize
    public boolean caseSensitive;


    public LogicalGraph(
            @Deserialize("id") long id,
            @Deserialize("name") String name,
            @Deserialize("modifiable") boolean modifiable,
            @Deserialize("caseSensitive") boolean caseSensitive ) {
        super( id, name, id, EntityType.ENTITY, NamespaceType.GRAPH, modifiable );
        this.caseSensitive = caseSensitive;
    }


    public LogicalGraph( LogicalGraph graph ) {
        this( graph.id, graph.name, graph.modifiable, graph.caseSensitive );
    }


    @Override
    public Serializable[] getParameterArray() {
        return new Serializable[0];
    }


    @Override
    public int compareTo( @NotNull LogicalGraph o ) {
        if ( o != null ) {
            return (int) (this.id - o.id);
        }
        return -1;
    }


    @Override
    public Expression asExpression() {
        return Expressions.call( Catalog.CATALOG_EXPRESSION, "getCollection", Expressions.constant( id ) );
    }


}
