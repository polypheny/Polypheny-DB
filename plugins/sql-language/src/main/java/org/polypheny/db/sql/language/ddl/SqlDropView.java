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
import org.polypheny.db.catalog.entity.logical.LogicalView;
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
 * Parse tree for {@code DROP VIEW} statement.
 */
public class SqlDropView extends SqlDropObject {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "DROP VIEW", Kind.DROP_VIEW );


    /**
     * Creates a SqlDropView.
     */
    SqlDropView( ParserPos pos, boolean ifExists, SqlIdentifier name ) {
        super( OPERATOR, pos, ifExists, name );
    }


    @Override
    public void execute( Context context, Statement statement, ParsedQueryContext parsedQueryContext ) {
        final Optional<? extends LogicalEntity> entity = searchEntity( context, name );

        if ( entity.isEmpty() ) {
            if ( ifExists ) {
                // It is ok that there is no view with this name because "IF EXISTS" was specified
                return;
            } else {
                throw new GenericRuntimeException( "Could not find the specified view: %s", name );
            }
        }

        Optional<LogicalView> optionalView = entity.get().unwrap( LogicalView.class );

        if ( optionalView.isEmpty() ) {
            throw new GenericRuntimeException( "Not possible to use DROP VIEW because " + name + " is not a view." );
        }

        LogicalView view = optionalView.get();

        if ( view.entityType != EntityType.VIEW ) {
            throw new GenericRuntimeException( "Not possible to use DROP VIEW because " + view.name + " is not a view." );
        }

        DdlManager.getInstance().dropView( view, statement );


    }

}
