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

import java.util.List;
import java.util.stream.Collectors;
import org.bson.BsonDocument;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.logistic.PlacementType;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.mql.Mql.Type;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.transaction.Statement;


public class MqlCreateCollection extends MqlNode implements ExecutableStatement {

    private final BsonDocument options;
    private final String name;


    public MqlCreateCollection( ParserPos pos, String name, String namespace, BsonDocument options ) {
        super( pos, namespace );
        this.name = name;
        this.options = options;
    }


    @Override
    public Type getMqlKind() {
        return Type.CREATE_COLLECTION;
    }


    @Override
    public String toString() {
        return "MqlCreateCollection{" +
                "name='" + name + '\'' +
                '}';
    }


    @Override
    public @Nullable String getEntity() {
        return name;
    }


    @Override
    public void execute( Context context, Statement statement, ParsedQueryContext parsedQueryContext ) {
        AdapterManager adapterManager = AdapterManager.getInstance();

        long namespaceId = parsedQueryContext.getNamespaceId();

        PlacementType placementType = PlacementType.AUTOMATIC;

        List<DataStore<?>> dataStores = stores
                .stream()
                .map( store -> adapterManager.getStore( store ).orElseThrow() )
                .collect( Collectors.toList() );
        DdlManager.getInstance().createCollection(
                namespaceId,
                name,
                true,
                dataStores.isEmpty() ? null : dataStores,
                placementType,
                statement );
    }

}
