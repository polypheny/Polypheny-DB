/*
 * Copyright 2019-2021 The Polypheny Project
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

package org.polypheny.db.mql;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.bson.BsonDocument;
import org.polypheny.db.adapter.AdapterManager;
import org.polypheny.db.adapter.DataStore;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.exceptions.TableAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownPartitionTypeException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.exception.ColumnNotExistsException;
import org.polypheny.db.ddl.exception.PartitionGroupNamesNotUniqueException;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.mql.Mql.Type;
import org.polypheny.db.mql.parser.MqlParserPos;
import org.polypheny.db.transaction.Statement;


public class MqlCreateCollection extends MqlNode implements MqlExecutableStatement {

    private final BsonDocument options;
    private final String name;


    public MqlCreateCollection( MqlParserPos pos, String name, BsonDocument options ) {
        super( pos );
        this.name = name;
        this.options = options;
    }


    @Override
    public Type getKind() {
        return Type.CREATE_COLLECTION;
    }


    @Override
    public String toString() {
        return "MqlCreateCollection{" +
                "name='" + name + '\'' +
                '}';
    }


    @Override
    public void execute( Context context, Statement statement, String database ) {
        Catalog catalog = Catalog.getInstance();
        AdapterManager adapterManager = AdapterManager.getInstance();

        long schemaId;
        try {
            schemaId = catalog.getSchema( Catalog.defaultDatabaseId, database ).id;
        } catch ( UnknownSchemaException e ) {
            throw new RuntimeException( "The used document database (Polypheny Schema) is not available." );
        }

        PlacementType placementType = PlacementType.AUTOMATIC;

        try {
            List<DataStore> dataStores = stores
                    .stream()
                    .map( store -> (DataStore) adapterManager.getAdapter( store ) )
                    .collect( Collectors.toList() );
            DdlManager.getInstance().createTable(
                    schemaId,
                    name,
                    new ArrayList<>(),
                    new ArrayList<>(),
                    true,
                    dataStores.size() == 0 ? null : dataStores,
                    placementType,
                    statement );
        } catch ( TableAlreadyExistsException | ColumnNotExistsException | UnknownPartitionTypeException | UnknownColumnException | PartitionGroupNamesNotUniqueException e ) {
            throw new RuntimeException( "The generation of the collection was not possible, due to: " + e.getMessage() );
        }
    }

}
