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

package org.polypheny.db.languages.mql;

import java.util.Collections;
import lombok.Getter;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.mql.Mql.Type;


public class MqlInsert extends MqlCollectionStatement {

    @Getter
    private final BsonArray values;
    @Getter
    private final boolean ordered;


    public MqlInsert( ParserPos pos, String collection, BsonValue values, BsonDocument options ) {
        super( collection, pos );
        if ( values.isDocument() ) {
            this.values = new BsonArray( Collections.singletonList( values.asDocument() ) );
        } else if ( values.isArray() ) {
            this.values = values.asArray();
        } else {
            throw new GenericRuntimeException( "Insert requires either a single document or multiple documents in an array." );
        }
        this.ordered = getBoolean( options, "ordered" );
    }


    @Override
    public Type getMqlKind() {
        return Type.INSERT;
    }


    @Override
    public @Nullable String getEntity() {
        return getCollection();
    }

}
