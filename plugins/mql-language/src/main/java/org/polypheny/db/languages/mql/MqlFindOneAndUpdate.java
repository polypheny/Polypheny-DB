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
import org.bson.BsonValue;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.mql.Mql.Type;


public class MqlFindOneAndUpdate extends MqlCollectionStatement implements MqlQueryStatement {

    @Getter
    private final BsonDocument query;
    @Getter
    private final BsonArray pipeline;
    @Getter
    private final Object update;
    @Getter
    private final boolean usesPipeline;
    @Getter
    private final BsonDocument projection;
    @Getter
    private final BsonDocument sort;
    @Getter
    private final boolean upsert;
    @Getter
    private final BsonDocument collation;


    public MqlFindOneAndUpdate( ParserPos pos, String collection, String namespace, BsonDocument query, BsonValue updateOrPipeline, BsonDocument options ) {
        super( collection, namespace, pos );
        this.query = query;
        if ( updateOrPipeline.isArray() ) {
            this.pipeline = updateOrPipeline.asArray();
            this.update = null;
            this.usesPipeline = true;
        } else {
            this.pipeline = null;
            this.update = updateOrPipeline.asDocument();
            this.usesPipeline = false;
        }
        this.projection = getDocumentOrNull( options, "projection" );
        this.sort = getDocumentOrNull( options, "sort" );
        this.upsert = getBoolean( options, "upsert" );
        this.collation = getDocumentOrNull( options, "collation" );
    }


    @Override
    public Type getMqlKind() {
        return Type.FIND_UPDATE;
    }


    @Override
    public @Nullable String getEntity() {
        return getCollection();
    }

}
