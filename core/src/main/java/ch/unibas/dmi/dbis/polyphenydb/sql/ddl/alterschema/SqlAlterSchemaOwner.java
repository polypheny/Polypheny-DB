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

package ch.unibas.dmi.dbis.polyphenydb.sql.ddl.alterschema;


import static ch.unibas.dmi.dbis.polyphenydb.util.Static.RESOURCE;

import ch.unibas.dmi.dbis.polyphenydb.Transaction;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogSchema;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogUser;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.GenericCatalogException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownSchemaException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownUserException;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.Context;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlUtil;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlWriter;
import ch.unibas.dmi.dbis.polyphenydb.sql.ddl.SqlAlterSchema;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableNullableList;
import java.util.List;
import java.util.Objects;


/**
 * Parse tree for {@code ALTER SCHEMA name OWNER TO} statement.
 */
public class SqlAlterSchemaOwner extends SqlAlterSchema {

    private final SqlIdentifier schema;
    private final SqlIdentifier owner;


    /**
     * Creates a SqlAlterSchemaOwner.
     */
    public SqlAlterSchemaOwner( SqlParserPos pos, SqlIdentifier schema, SqlIdentifier owner ) {
        super( pos );
        this.schema = Objects.requireNonNull( schema );
        this.owner = Objects.requireNonNull( owner );
    }


    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of( schema, owner );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "ALTER" );
        writer.keyword( "SCHEMA" );
        schema.unparse( writer, leftPrec, rightPrec );
        writer.keyword( "OWNER" );
        writer.keyword( "TO" );
        owner.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Transaction transaction ) {
        try {
            CatalogSchema catalogSchema = transaction.getCatalog().getSchema( context.getDatabaseId(), schema.getSimple() );
            CatalogUser catalogUser = transaction.getCatalog().getUser( owner.getSimple() );
            transaction.getCatalog().setSchemaOwner( catalogSchema.id, catalogUser.id );
        } catch ( GenericCatalogException e ) {
            throw new RuntimeException( e );
        } catch ( UnknownSchemaException e ) {
            throw SqlUtil.newContextException( schema.getParserPosition(), RESOURCE.schemaNotFound( schema.getSimple() ) );
        } catch ( UnknownUserException e ) {
            throw SqlUtil.newContextException( owner.getParserPosition(), RESOURCE.userNotFound( owner.getSimple() ) );
        }
    }

}

