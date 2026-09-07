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

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.deser.ContextualDeserializer;
import java.io.IOException;

public class FieldDeserializer extends JsonDeserializer<Field<?>> implements ContextualDeserializer {
    private final String key;
    private final JavaType valueType;

    public FieldDeserializer() {
        this.key = null;
        this.valueType = null;
    }

    private FieldDeserializer(String key, JavaType valueType) {
        this.key = key;
        this.valueType = valueType;
    }

    @Override
    public JsonDeserializer<?> createContextual(DeserializationContext ctxt, BeanProperty property) {
        String propertyKey = property != null ? property.getName().replace( "-", "_" ).replace( "'", "" ) : null;
        JavaType wrapperType = property != null ? property.getType() : ctxt.getContextualType();
        JavaType innerType = wrapperType.containedType(0);
        return new FieldDeserializer(propertyKey, innerType);
    }

    @Override
    public Field<?> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        Object rawValue = ctxt.readValue(p, valueType);
        return new Field<>(key, valueType, rawValue);
    }

    @Override
    public Field<?> getNullValue(DeserializationContext ctxt) {
        // Called instead of deserialize() when the JSON value is literally null.
        // Return a Field wrapping a null value, not a null Field.
        return new Field<>(key, valueType, null);
    }
}


