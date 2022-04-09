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

package org.polypheny.db.schema.graph;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.util.List;
import java.util.UUID;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.experimental.Accessors;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.polypheny.db.adapter.enumerable.EnumUtils;
import org.polypheny.db.runtime.PolyCollections;
import org.polypheny.db.runtime.PolyCollections.PolyDictionary;
import org.polypheny.db.runtime.PolyCollections.PolyList;
import org.polypheny.db.tools.ExpressionTransformable;

@Getter
public class PolyNode extends GraphPropertyHolder implements Comparable<PolyNode>, ExpressionTransformable {

    @Getter
    @Setter
    @Accessors(fluent = true)
    private boolean isVariable = false;


    public PolyNode( @NonNull PolyCollections.PolyDictionary properties, List<String> labels, String variableName ) {
        this( UUID.randomUUID().toString(), properties, labels, variableName );
    }


    public PolyNode( String id, @NonNull PolyCollections.PolyDictionary properties, List<String> labels, String variableName ) {
        super( id, GraphObjectType.NODE, properties, labels, variableName );
    }


    @Override
    public int compareTo( PolyNode o ) {
        return id.compareTo( o.id ); //getProperties().compareTo( o.getProperties() );
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
    public Expression getAsExpression() {
        return Expressions.call( Expressions.convert_(
                Expressions.new_(
                        PolyNode.class,
                        Expressions.constant( id ),
                        properties.getAsExpression(),
                        EnumUtils.constantArrayList( labels, String.class ),
                        Expressions.constant( getVariableName(), String.class ) ),
                PolyNode.class
        ), "isVariable", Expressions.constant( true ) );
    }


    @Override
    public void setLabels( List<String> labels ) {
        this.labels.addAll( labels );
    }


    public PolyNode copyNamed( String variableName ) {
        if ( variableName == null ) {
            // no copy needed
            return this;
        }
        return new PolyNode( id, properties, labels, variableName );

    }


    static public class PolyNodeSerializer extends Serializer<PolyNode> {

        @Override
        public void write( Kryo kryo, Output output, PolyNode object ) {
            kryo.writeClassAndObject( output, object.id );
            kryo.writeClassAndObject( output, object.properties );
            kryo.writeClassAndObject( output, object.labels );
            kryo.writeClassAndObject( output, object.isVariable );
        }


        @Override
        public PolyNode read( Kryo kryo, Input input, Class<? extends PolyNode> type ) {
            String id = (String) kryo.readClassAndObject( input );
            PolyDictionary properties = (PolyDictionary) kryo.readClassAndObject( input );
            PolyList<String> labels = (PolyList<String>) kryo.readClassAndObject( input );
            String variableName = (String) kryo.readClassAndObject( input );
            boolean isVariable = (boolean) kryo.readClassAndObject( input );
            return new PolyNode( id, properties, labels, variableName )
                    .isVariable( isVariable );
        }

    }

}
