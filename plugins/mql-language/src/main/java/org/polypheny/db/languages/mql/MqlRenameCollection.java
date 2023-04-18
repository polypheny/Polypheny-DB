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

package org.polypheny.db.languages.mql;

import java.util.List;
import java.util.Optional;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.logistic.Pattern;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryParameters;
import org.polypheny.db.languages.mql.Mql.Type;
import org.polypheny.db.nodes.ExecutableStatement;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.transaction.Statement;


public class MqlRenameCollection extends MqlCollectionStatement implements ExecutableStatement {

    private final String newName;
    private final boolean dropTarget;


    public MqlRenameCollection( ParserPos pos, String collection, String newName, Boolean dropTarget ) {
        super( collection, pos );
        this.newName = newName;
        this.dropTarget = dropTarget != null && dropTarget;
    }


    @Override
    public Type getMqlKind() {
        return Type.RENAME_COLLECTION;
    }


    @Override
    public void execute( Context context, Statement statement, QueryParameters parameters ) {
        String database = ((MqlQueryParameters) parameters).getDatabase();

        List<LogicalTable> tables = context.getSnapshot().rel().getTables( Pattern.of( database ), null );

        if ( dropTarget ) {
            Optional<LogicalTable> newTable = tables.stream()
                    .filter( t -> t.name.equals( newName ) )
                    .findAny();

            newTable.ifPresent( logicalTable -> DdlManager.getInstance().dropTable( logicalTable, statement ) );
        }

        Optional<LogicalTable> table = tables.stream()
                .filter( t -> t.name.equals( getCollection() ) )
                .findAny();

        if ( table.isEmpty() ) {
            throw new RuntimeException( "The target for the rename is not valid." );
        }

        DdlManager.getInstance().renameTable( table.get(), newName, statement );

    }

}
