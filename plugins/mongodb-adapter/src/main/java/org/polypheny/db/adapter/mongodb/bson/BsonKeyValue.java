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

package org.polypheny.db.adapter.mongodb.bson;

import java.util.function.Function;
import lombok.EqualsAndHashCode;
import lombok.Value;
import org.bson.BsonDocument;
import org.bson.BsonValue;

@EqualsAndHashCode(callSuper = true)
@Value
public class BsonKeyValue extends BsonDocument {

    public static final String PLACEHOLDER_KEY = "_kv_";
    public static final String KEY_KEY = "_k_";
    public static final String VALUE_KEY = "_v_";
    public String placeholderKey;


    public BsonKeyValue( BsonValue key, BsonValue value ) {
        super();
        append( KEY_KEY, key );
        append( VALUE_KEY, value );
        this.placeholderKey = PLACEHOLDER_KEY + key.hashCode();
    }


    public BsonValue wrapValue( Function<BsonValue, BsonValue> wrapper ) {
        return append( VALUE_KEY, wrapper.apply( get( VALUE_KEY ) ) );
    }

}
