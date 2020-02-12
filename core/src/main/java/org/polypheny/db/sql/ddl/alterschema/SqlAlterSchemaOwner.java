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

package org.polypheny.db.sql.ddl.alterschema;


import static org.polypheny.db.util.Static.RESOURCE;

import java.util.List;
import java.util.Objects;
import org.polypheny.db.Transaction;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogUser;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.catalog.exceptions.UnknownUserException;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.ddl.SqlAlterSchema;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.util.ImmutableNullableList;


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

