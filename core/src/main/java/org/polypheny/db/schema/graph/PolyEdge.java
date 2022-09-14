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

import com.google.gson.annotations.Expose;
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
import org.polypheny.db.tools.ExpressionTransformable;
import org.polypheny.db.util.BuiltInMethod;
import org.polypheny.db.util.Pair;


@Getter
public class PolyEdge extends GraphPropertyHolder implements Comparable<PolyEdge>, ExpressionTransformable {

    @Expose
    public final String source;
    @Expose
    public final String target;
    @Expose
    public final EdgeDirection direction;

    @Getter
    @Setter
    @Accessors(fluent = true)
    private Pair<Integer, Integer> fromTo;


    public PolyEdge( @NonNull PolyCollections.PolyDictionary properties, List<String> labels, String source, String target, EdgeDirection direction, String variableName ) {
        this( UUID.randomUUID().toString(), properties, labels, source, target, direction, variableName );
    }


    public PolyEdge( String id, @NonNull PolyCollections.PolyDictionary properties, List<String> labels, String source, String target, EdgeDirection direction, String variableName ) {
        super( id, GraphObjectType.EDGE, properties, labels, variableName );
        this.source = source;
        this.target = target;
        this.direction = direction;
    }


    public int getVariants() {
        if ( fromTo == null ) {
            return 1;
        }
        return fromTo.right - fromTo.left + 1;
    }


    @Override
    public int compareTo( PolyEdge other ) {
        return id.compareTo( other.id );
    }


    public PolyEdge from( String left, String right ) {
        return new PolyEdge( id, properties, labels, left == null ? this.source : left, right == null ? this.target : right, direction, null );
    }


    @Override
    public void setLabels( List<String> labels ) {
        this.labels.clear();
        this.labels.add( labels.get( 0 ) );
    }


    @Override
    public Expression getAsExpression() {
        Expression expression =
                Expressions.convert_(
                        Expressions.new_(
                                PolyEdge.class,
                                Expressions.constant( id ),
                                properties.getAsExpression(),
                                EnumUtils.constantArrayList( labels, String.class ),
                                Expressions.constant( source ),
                                Expressions.constant( target ),
                                Expressions.constant( direction ),
                                Expressions.constant( getVariableName(), String.class ) ),
                        PolyEdge.class );
        if ( fromTo != null ) {
            expression = Expressions.call( expression, "fromTo",
                    Expressions.call( BuiltInMethod.PAIR_OF.method, Expressions.constant( fromTo.left ), Expressions.constant( fromTo.right ) ) );
        }
        return expression;
    }


    public PolyEdge copyNamed( String newName ) {
        if ( newName == null ) {
            // no copy needed
            return this;
        }
        return new PolyEdge( id, properties, labels, source, target, direction, newName );
    }


    public boolean isRange() {
        if ( fromTo == null ) {
            return false;
        }
        return !fromTo.left.equals( fromTo.right );
    }


    public int getMinLength() {
        if ( fromTo == null ) {
            return 1;
        }
        return fromTo.left;
    }


    public String getRangeDescriptor() {
        if ( fromTo == null ) {
            return "";
        }
        String range = "*";

        if ( fromTo.left != null && fromTo.right != null ) {
            if ( fromTo.left.equals( fromTo.right ) ) {
                return range + fromTo.right;
            }
            return range + fromTo.left + ".." + fromTo.right;
        }
        if ( fromTo.right != null ) {
            return fromTo.right.toString();
        }

        if ( fromTo.left != null ) {
            return fromTo.left.toString();
        }
        return range;
    }


    public enum EdgeDirection {
        LEFT_TO_RIGHT,
        RIGHT_TO_LEFT,
        NONE
    }


    @Override
    public String toString() {
        return "PolyEdge{" +
                "id=" + id +
                ", properties=" + properties +
                ", labels=" + labels +
                ", leftId=" + source +
                ", rightId=" + target +
                ", direction=" + direction +
                '}';
    }


}
