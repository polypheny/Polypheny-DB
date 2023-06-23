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

package org.polypheny.db.type.entity;

import java.math.BigDecimal;
import lombok.NonNull;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.runtime.PolyCollections.FlatMap;
import org.polypheny.db.type.PolySerializable;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.category.PolyBlob;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.type.entity.category.PolyTemporal;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyGraph;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.graph.PolyPath;
import org.polypheny.db.type.entity.relational.PolyMap;

public class PolyNull extends PolyValue {

    public static PolyNull NULL = new PolyNull();


    private PolyNull() {
        super( PolyType.NULL );
    }


    @Override
    public int compareTo( @NotNull PolyValue o ) {
        return o.isNull() ? 0 : -1;
    }


    @Override
    public boolean isBoolean() {
        return true;
    }


    @Override
    public @NonNull PolyBoolean asBoolean() {
        return PolyBoolean.of( null );
    }


    @Override
    public boolean isInteger() {
        return true;
    }


    @Override
    public @NonNull PolyInteger asInteger() {
        return PolyInteger.of( (Integer) null );
    }


    @Override
    public boolean isDocument() {
        return true;
    }


    @Override
    public @NonNull PolyDocument asDocument() {
        return PolyDocument.ofDocument( null );
    }


    @Override
    public boolean isList() {
        return true;
    }


    @Override
    public @NonNull <T extends PolyValue> PolyList<T> asList() {
        return PolyList.of();
    }


    @Override
    public boolean isString() {
        return true;
    }


    @Override
    public @NonNull PolyString asString() {
        return PolyString.of( null );
    }


    @Override
    public boolean isBinary() {
        return true;
    }


    @Override
    public @NonNull PolyBinary asBinary() {
        return PolyBinary.of( (ByteString) null );
    }


    @Override
    public boolean isBigDecimal() {
        return true;
    }


    @Override
    public @NonNull PolyBigDecimal asBigDecimal() {
        return PolyBigDecimal.of( (BigDecimal) null );
    }


    @Override
    public boolean isFloat() {
        return true;
    }


    @Override
    public @NonNull PolyFloat asFloat() {
        return PolyFloat.of( (Float) null );
    }


    @Override
    public boolean isDouble() {
        return true;
    }


    @Override
    public @NonNull PolyDouble asDouble() {
        return PolyDouble.of( (Double) null );
    }


    @Override
    public boolean isLong() {
        return true;
    }


    @Override
    public @NonNull PolyLong asLong() {
        return PolyLong.of( (Long) null );
    }


    @Override
    public boolean isTemporal() {
        return true;
    }


    @Override
    public PolyTemporal asTemporal() {
        return PolyDate.of( (Long) null );
    }


    @Override
    public boolean isDate() {
        return true;
    }


    @Override
    public @NonNull PolyDate asDate() {
        return PolyDate.of( (Long) null );
    }


    @Override
    public boolean isTime() {
        return true;
    }


    @Override
    public @NonNull PolyTime asTime() {
        return (PolyTime) PolyTime.of( (Long) null );
    }


    @Override
    public boolean isTimestamp() {
        return true;
    }


    @Override
    public @NonNull PolyTimeStamp asTimeStamp() {
        return PolyTimeStamp.of( (Long) null );
    }


    @Override
    public boolean isMap() {
        return true;
    }


    @Override
    public @NonNull PolyMap<PolyValue, PolyValue> asMap() {
        return PolyMap.of( null );
    }


    @Override
    public boolean isEdge() {
        return true;
    }


    @Override
    public @NonNull PolyEdge asEdge() {
        return new PolyEdge( new PolyDictionary(), null, null, null, null, null );
    }


    @Override
    public boolean isNode() {
        return true;
    }


    @Override
    public @NonNull PolyNode asNode() {
        return new PolyNode( new PolyDictionary(), null, null );
    }


    @Override
    public boolean isPath() {
        return true;
    }


    @Override
    public @NonNull PolyPath asPath() {
        return new PolyPath( null, null, null, null, null );
    }


    @Override
    public boolean isGraph() {
        return true;
    }


    @Override
    public @NonNull PolyGraph asGraph() {
        return new PolyGraph( null, new FlatMap<>(), new FlatMap<>() );
    }


    @Override
    public boolean isNumber() {
        return true;
    }


    @Override
    public PolyNumber asNumber() {
        return PolyInteger.of( null );
    }


    @Override
    public boolean isInterval() {
        return true;
    }


    @Override
    public PolyInterval asInterval() {
        return PolyInterval.of( null, null );
    }


    @Override
    public boolean isSymbol() {
        return true;
    }


    @Override
    public PolySymbol asSymbol() {
        return PolySymbol.of( null );
    }


    @Override
    public boolean isBlob() {
        return true;
    }


    @Override
    public @NonNull PolyBlob asBlob() {
        return new PolyBlob( null );
    }


    @Override
    public Expression asExpression() {
        return Expressions.new_( PolyNull.class );
    }


    @Override
    public PolySerializable copy() {
        return PolySerializable.deserialize( serialize(), PolyNull.class );
    }

}
