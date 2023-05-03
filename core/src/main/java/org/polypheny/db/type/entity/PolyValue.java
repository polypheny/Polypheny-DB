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

import lombok.Value;
import lombok.experimental.NonFinal;
import org.polypheny.db.schema.types.Expressible;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.document.PolyBoolean;
import org.polypheny.db.type.entity.document.PolyDocument;
import org.polypheny.db.type.entity.document.PolyInteger;
import org.polypheny.db.type.entity.document.PolyList;

@Value
@NonFinal
public abstract class PolyValue<T> implements Expressible, Comparable<PolyValue<T>> {

    public PolyType type;

    public T value;


    public boolean isSameType( PolyValue<?> value ) {
        return type == value.type;
    }


    public boolean isBoolean() {
        return type == PolyType.BOOLEAN;
    }


    public PolyBoolean asBoolean() {
        if ( isBoolean() ) {
            return (PolyBoolean) this;
        }
        return null;
    }


    public boolean isInteger() {
        return type == PolyType.INTEGER;
    }


    public PolyInteger asInteger() {
        if ( isInteger() ) {
            return (PolyInteger) this;
        }
        return null;
    }


    public boolean isDocument() {
        return type == PolyType.DOCUMENT;
    }


    public PolyDocument asDocument() {
        if ( isDocument() ) {
            return (PolyDocument) this;
        }
        return null;
    }


    public boolean isList() {
        return type == PolyType.ARRAY;
    }


    public PolyList asList() {
        if ( isList() ) {
            return (PolyList) this;
        }
        return null;
    }


}
