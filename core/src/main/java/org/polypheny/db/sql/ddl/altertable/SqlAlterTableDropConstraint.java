/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.sql.ddl.altertable;


import java.util.List;
import java.util.Objects;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogConstraint;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownConstraintException;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.ddl.SqlAlterTable;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code ALTER TABLE name DROP CONSTRAINT} statement.
 */
public class SqlAlterTableDropConstraint extends SqlAlterTable {

    private final SqlIdentifier table;
    private final SqlIdentifier constraintName;


    public SqlAlterTableDropConstraint( SqlParserPos pos, SqlIdentifier table, SqlIdentifier constraintName ) {
        super( pos );
        this.table = Objects.requireNonNull( table );
        this.constraintName = Objects.requireNonNull( constraintName );
    }


    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of( table, constraintName );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "TABLE" );
        table.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "DROP" );
        writer.keyword( "CONSTRAINT" );
        constraintName.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Transaction transaction ) {
        CatalogTable catalogTable = getCatalogTable( context, table );
        try {
            CatalogConstraint constraint = Catalog.getInstance().getConstraint( catalogTable.id, constraintName.getSimple() );
            Catalog.getInstance().deleteConstraint( constraint.id );
        } catch ( GenericCatalogException | UnknownConstraintException e ) {
            throw new RuntimeException( e );
        }
    }

}

