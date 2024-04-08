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

import lombok.Getter;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.mql.Mql.Type;


public class MqlAggregate extends MqlCollectionStatement {

    @Getter
    private final BsonArray pipeline;
    @Getter
    private final BsonDocument option;


    public MqlAggregate( ParserPos pos, String collection, String namespace, BsonArray pipeline, BsonDocument option ) {
        super( collection, namespace, pos );
        this.pipeline = pipeline;
        this.option = option;
        enforceNonEmptyProject( pipeline );
    }


    private void enforceNonEmptyProject( BsonArray pipeline ) {

    }


    @Override
    public Type getMqlKind() {
        return Type.AGGREGATE;
    }


    @Override
    public @Nullable String getEntity() {
        return getCollection();
    }

}
