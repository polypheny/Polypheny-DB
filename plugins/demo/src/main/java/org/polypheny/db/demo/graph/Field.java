/*
 * Copyright 2019-2026 The Polypheny Project
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

package org.polypheny.db.demo.graph;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.type.TypeFactory;
import java.util.List;
import java.util.stream.Collectors;

@JsonDeserialize(using = FieldDeserializer.class)
public class Field<T> {
    private final String key;
    private final JavaType type;
    private T value;

    public boolean isNull() {
        return this.value == null;
    }

    public String format(Object value, JavaType type) {
        if (value == null) {
            return "null";
        }

        if ( type.hasRawClass( String.class ) ) {
            return String.format( "\"%s\"", value );
        }
        else if ( type.hasRawClass( Boolean.class ) || type.hasRawClass( Integer.class ) || type.hasRawClass( Float.class ) || type.hasRawClass( Double.class )) {
            return String.valueOf( value );
        }
        else if ( type.isCollectionLikeType() ) {
            JavaType innerType = type.getContentType();
            List<?> list = (List<?>) value;
            return list.stream().map( item -> format( item, innerType ) ).collect( Collectors.joining(", ", "[", "]") );
        }
        else if ( value instanceof CypherObject ) {
            return ((CypherObject) value).query();
        }

        return "";
    }

    public String get() {
        return String.format( "%s: %s", this.key, this.format( this.value, this.type ) );
    }

    public Field( String key, Class<T> type ) {
        this.key = key;
        this.type = TypeFactory.defaultInstance().constructType( type );
    }

    public Field( String key, TypeReference<T> type ) {
        this.key = key;
        this.type = TypeFactory.defaultInstance().constructType( type );
    }

    public Field(String key, JavaType type, T value) {
        this.key = key;
        this.type = type;
        this.value = value;
    }
}
