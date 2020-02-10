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

package ch.unibas.dmi.dbis.polyphenydb.sql.ddl.altertable;


import static ch.unibas.dmi.dbis.polyphenydb.util.Static.RESOURCE;

import ch.unibas.dmi.dbis.polyphenydb.Transaction;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogTable;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogUser;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.GenericCatalogException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownUserException;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.Context;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlUtil;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlWriter;
import ch.unibas.dmi.dbis.polyphenydb.sql.ddl.SqlAlterTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableNullableList;
import java.util.List;
import java.util.Objects;


/**
 * Parse tree for {@code ALTER TABLE name OWNER TO} statement.
 */
public class SqlAlterTableOwner extends SqlAlterTable {

    private final SqlIdentifier table;
    private final SqlIdentifier owner;


    public SqlAlterTableOwner( SqlParserPos pos, SqlIdentifier table, SqlIdentifier owner ) {
        super( pos );
        this.table = Objects.requireNonNull( table );
        this.owner = Objects.requireNonNull( owner );
    }


    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of( table, owner );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "TABLE" );
        table.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "OWNER" );
        writer.keyword( "TO" );
        owner.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Transaction transaction ) {
        CatalogTable catalogTable = getCatalogTable( context, transaction, table );

        if ( owner.names.size() != 1 ) {
            throw new RuntimeException( "No FQDN allowed here: " + owner.toString() );
        }
        String newOwnerName = owner.getSimple();
        try {
            CatalogUser catalogUser = transaction.getCatalog().getUser( newOwnerName );
            transaction.getCatalog().setTableOwner( catalogTable.id, catalogUser.id );
        } catch ( GenericCatalogException e ) {
            throw new RuntimeException( e );
        } catch ( UnknownUserException e ) {
            throw SqlUtil.newContextException( owner.getParserPosition(), RESOURCE.userNotFound( owner.getSimple() ) );
        }
    }


}

