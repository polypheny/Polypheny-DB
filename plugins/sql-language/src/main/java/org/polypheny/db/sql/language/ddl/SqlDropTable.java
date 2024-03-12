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

package org.polypheny.db.sql.language.ddl;


import java.util.Optional;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.catalog.entity.logical.LogicalEntity;
import org.polypheny.db.catalog.entity.logical.LogicalTable;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.catalog.logistic.EntityType;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.prepare.Context;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.sql.language.SqlIdentifier;
import org.polypheny.db.sql.language.SqlOperator;
import org.polypheny.db.sql.language.SqlSpecialOperator;
import org.polypheny.db.transaction.Statement;


/**
 * Parse tree for {@code DROP TABLE} statement.
 */
public class SqlDropTable extends SqlDropObject {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "DROP TABLE", Kind.DROP_TABLE );


    /**
     * Creates a SqlDropTable.
     */
    SqlDropTable( ParserPos pos, boolean ifExists, SqlIdentifier name ) {
        super( OPERATOR, pos, ifExists, name );
    }


    @Override
    public void execute( Context context, Statement statement, ParsedQueryContext parsedQueryContext ) {
        final Optional<? extends LogicalEntity> entity = searchEntity( context, name );

        if ( entity.isEmpty() ) {
            if ( ifExists ) {
                // It is ok that there is no table with this name because "IF EXISTS" was specified
                return;
            } else {
                throw new GenericRuntimeException( "There exists no entity with the name %s and 'IF EXISTS' was not specified", name );
            }
        }

        Optional<LogicalTable> optionalTable = entity.get().unwrap( LogicalTable.class );

        if ( optionalTable.isEmpty() ) {
            throw new GenericRuntimeException( "The entity with the name %s is not a relational entity % but of type: ", name, entity.get().dataModel );
        }
        LogicalTable table = optionalTable.get();

        if ( table.entityType != EntityType.ENTITY && table.entityType != EntityType.SOURCE ) {
            throw new GenericRuntimeException( "The entity with the name %s is of %s use the appropriate query.", name, table.entityType );
        }

        DdlManager.getInstance().dropTable( table, statement );
    }

}
