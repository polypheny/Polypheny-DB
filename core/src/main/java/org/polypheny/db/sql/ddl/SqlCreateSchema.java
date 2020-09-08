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

import java.util.List;
import java.util.Objects;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.SchemaType;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.sql.SqlCreate;
import org.polypheny.db.sql.SqlExecutableStatement;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.sql.SqlSpecialOperator;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.parser.SqlParserPos;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code CREATE SCHEMA} statement.
 */
public class SqlCreateSchema extends SqlCreate implements SqlExecutableStatement {

    private final SqlIdentifier name;

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "CREATE SCHEMA", SqlKind.CREATE_SCHEMA );


    /**
     * Creates a SqlCreateSchema.
     */
    SqlCreateSchema( SqlParserPos pos, boolean replace, boolean ifNotExists, SqlIdentifier name ) {
        super( OPERATOR, pos, replace, ifNotExists );
        this.name = Objects.requireNonNull( name );
    }


    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of( name );
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        writer.keyword( "CREATE" );
        if ( replace ) {
            writer.keyword( "OR REPLACE" );
        }
        writer.keyword( "SCHEMA" );
        if ( ifNotExists ) {
            writer.keyword( "IF NOT EXISTS" );
        }
        name.unparse( writer, leftPrec, rightPrec );
    }


    @Override
    public void execute( Context context, Statement statement ) {
        try {
            Catalog catalog = Catalog.getInstance();
            // Check if there is already a schema with this name
            if ( catalog.checkIfExistsSchema( context.getDatabaseId(), name.getSimple() ) ) {
                if ( ifNotExists ) {
                    // It is ok that there is already a schema with this name because "IF NOT EXISTS" was specified
                    return;
                } else if ( replace ) {
                    throw new RuntimeException( "Replacing schema is not yet supported." );
                } else {
                    throw SqlUtil.newContextException( name.getParserPosition(), RESOURCE.schemaExists( name.getSimple() ) );
                }
            } else {
                catalog.addSchema(
                        name.getSimple(),
                        context.getDatabaseId(),
                        context.getCurrentUserId(),
                        SchemaType.RELATIONAL );
            }
        } catch ( GenericCatalogException | UnknownSchemaException e ) {
            throw new RuntimeException( e );
        }
    }

}

