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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.sql.ddl;


import static org.polypheny.db.util.Static.RESOURCE;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.entity.CatalogSchema;
import org.polypheny.db.catalog.entity.CatalogTable;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.sql.SqlDrop;
import org.polypheny.db.sql.SqlExecutableStatement;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.sql.SqlSpecialOperator;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.transaction.Transaction;


/**
 * Parse tree for {@code DROP SCHEMA} statement.
 */
public class SqlDropSchema extends SqlDrop implements SqlExecutableStatement {

    private final SqlIdentifier name;

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "DROP SCHEMA", SqlKind.DROP_TABLE );


    /**
     * Creates a SqlDropSchema.
     */
    SqlDropSchema( SqlParserPos pos, boolean ifExists, SqlIdentifier name ) {
        super( OPERATOR, pos, ifExists );
        this.name = name;
    }


    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of( name );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "DROP SCHEMA" );
        if ( ifExists ) {
            writer.keyword( "IF EXISTS" );
        }
        name.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Transaction transaction ) {
        try {
            Catalog catalog = Catalog.getInstance();
            // Check if there is a schema with this name
            if ( catalog.checkIfExistsSchema( context.getDatabaseId(), name.getSimple() ) ) {
                CatalogSchema catalogSchema = catalog.getSchema( context.getDatabaseId(), name.getSimple() );

                // Drop all tables in this schema
                List<CatalogTable> catalogTables = catalog.getTables( catalogSchema.id, null );
                catalogTables.forEach( catalogTable -> {
                    new SqlDropTable(
                            SqlParserPos.ZERO,
                            false,
                            new SqlIdentifier( Arrays.asList( catalogTable.getDatabaseName(), catalogTable.getSchemaName(), catalogTable.name ), SqlParserPos.ZERO )
                    ).execute( context, transaction );
                } );

                // Drop schema
                catalog.deleteSchema( catalogSchema.id );
            } else {
                if ( ifExists ) {
                    // This is ok because "IF EXISTS" was specified
                    return;
                } else {
                    throw SqlUtil.newContextException( name.getParserPosition(), RESOURCE.schemaNotFound( name.getSimple() ) );
                }
            }
        } catch ( GenericCatalogException | UnknownSchemaException e ) {
            throw new RuntimeException( e );
        }
    }

}
