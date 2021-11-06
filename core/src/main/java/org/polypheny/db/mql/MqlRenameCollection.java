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

import java.util.List;
import java.util.Optional;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.TableAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.ddl.exception.DdlOnSourceException;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.mql.Mql.Type;
import org.polypheny.db.mql.parser.MqlParserPos;
import org.polypheny.db.transaction.Statement;


public class MqlRenameCollection extends MqlCollectionStatement implements MqlExecutableStatement {

    private final String newName;
    private final boolean dropTarget;


    public MqlRenameCollection( MqlParserPos pos, String collection, String newName, Boolean dropTarget ) {
        super( collection, pos );
        this.newName = newName;
        this.dropTarget = dropTarget != null && dropTarget;
    }


    @Override
    public Type getKind() {
        return Type.RENAME_COLLECTION;
    }


    @Override
    public void execute( Context context, Statement statement, String database ) {
        Catalog catalog = Catalog.getInstance();

        try {
            CatalogSchema schema = catalog.getSchema( Catalog.defaultDatabaseId, database );
            List<CatalogTable> tables = catalog.getTables( schema.id, null );

            if ( dropTarget ) {
                Optional<CatalogTable> newTable = tables.stream()
                        .filter( t -> t.name.equals( newName ) )
                        .findAny();

                if ( newTable.isPresent() ) {
                    DdlManager.getInstance().dropTable( newTable.get(), statement );
                }
            }

            Optional<CatalogTable> table = tables.stream()
                    .filter( t -> t.name.equals( getCollection() ) )
                    .findAny();

            if ( !table.isPresent() ) {
                throw new RuntimeException( "The target for the rename is not valid." );
            }

            DdlManager.getInstance().renameTable( table.get(), newName, statement );
        } catch ( DdlOnSourceException | TableAlreadyExistsException | UnknownSchemaException e ) {
            throw new RuntimeException( "The rename was not successful, due to an error: " + e.getMessage() );
        }
    }

}
