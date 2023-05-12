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

package org.polypheny.db.type.entity.graph;

import java.util.UUID;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.algebra.enumerable.EnumUtils;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;


@Getter
public class PolyNode extends GraphPropertyHolder {

    @Getter
    @Setter
    @Accessors(fluent = true)
    private boolean isVariable = false;


    public PolyNode( @NonNull PolyDictionary properties, PolyList<PolyString> labels, PolyString variableName ) {
        this( PolyString.of( UUID.randomUUID().toString() ), properties, labels, variableName );
    }


    public PolyNode( PolyString id, @NonNull PolyDictionary properties, PolyList<PolyString> labels, PolyString variableName ) {
        super( id, PolyType.NODE, properties, labels, variableName );
    }


    @Override
    public String toString() {
        return "PolyNode{" +
                "id=" + id +
                ", properties=" + properties +
                ", labels=" + labels +
                '}';
    }


    public boolean isBlank() {
        // MATCH (n) -> true, MATCH (n{name: 'Max'}) -> false, MATCH (n:Person) -> false
        return (properties == null || properties.isEmpty()) && (labels == null || labels.isEmpty());
    }


    @Override
    public Expression asExpression() {
        return Expressions.call( Expressions.convert_(
                Expressions.new_(
                        PolyNode.class,
                        Expressions.constant( id ),
                        properties.asExpression(),
                        EnumUtils.constantArrayList( labels, String.class ),
                        Expressions.constant( getVariableName(), String.class ) ),
                PolyNode.class
        ), "isVariable", Expressions.constant( true ) );
    }


    @Override
    public void setLabels( PolyList<PolyString> labels ) {
        this.labels.addAll( labels );
    }


    public PolyNode copyNamed( PolyString variableName ) {
        if ( variableName == null ) {
            // no copy needed
            return this;
        }
        return new PolyNode( id, properties, labels, variableName );

    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        if ( !isNode() ) {
            return -1;
        }

        return id.compareTo( o.asNode().id );
    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), PolyNode.class );
    }

}
