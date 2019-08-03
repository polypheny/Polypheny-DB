/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.sql.ddl;


import static ch.unibas.dmi.dbis.polyphenydb.util.Static.RESOURCE;

import ch.unibas.dmi.dbis.polyphenydb.Transaction;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogSchema;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogTable;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.GenericCatalogException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownSchemaException;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.Context;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDrop;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlExecutableStatement;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlKind;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlSpecialOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlUtil;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlWriter;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;


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
            // Check if there is a schema with this name
            if ( transaction.getCatalog().checkIfExistsSchema( context.getDatabaseId(), name.getSimple() ) ) {
                CatalogSchema catalogSchema = transaction.getCatalog().getSchema( context.getDatabaseId(), name.getSimple() );

                // Drop all tables in this schema
                List<CatalogTable> catalogTables = transaction.getCatalog().getTables( catalogSchema.id, null );
                catalogTables.forEach( catalogTable -> {
                    new SqlDropTable(
                            SqlParserPos.ZERO,
                            false,
                            new SqlIdentifier( Arrays.asList( catalogTable.databaseName, catalogTable.schemaName, catalogTable.name ), SqlParserPos.ZERO )
                    ).execute( context, transaction );
                } );

                // Drop schema
                transaction.getCatalog().deleteSchema( catalogSchema.id );
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
