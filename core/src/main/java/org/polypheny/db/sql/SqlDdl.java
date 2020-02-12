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

package ch.unibas.dmi.dbis.polyphenydb.sql;


import static ch.unibas.dmi.dbis.polyphenydb.util.Static.RESOURCE;

import ch.unibas.dmi.dbis.polyphenydb.Transaction;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogColumn;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogTable;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedTable;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.GenericCatalogException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownCollationException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownColumnException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownDatabaseException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownSchemaException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownSchemaTypeException;
import ch.unibas.dmi.dbis.polyphenydb.catalog.exceptions.UnknownTableException;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.Context;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import java.util.Objects;


/**
 * Base class for CREATE, DROP and other DDL statements.
 */
public abstract class SqlDdl extends SqlCall {

    /**
     * Use this operator only if you don't have a better one.
     */
    protected static final SqlOperator DDL_OPERATOR = new SqlSpecialOperator( "DDL", SqlKind.OTHER_DDL );

    private final SqlOperator operator;


    /**
     * Creates a SqlDdl.
     */
    public SqlDdl( SqlOperator operator, SqlParserPos pos ) {
        super( pos );
        this.operator = Objects.requireNonNull( operator );
    }


    @Override
    public SqlOperator getOperator() {
        return operator;
    }


    protected CatalogTable getCatalogTable( Context context, Transaction transaction, SqlIdentifier tableName ) {
        CatalogTable catalogTable;
        try {
            long schemaId;
            String tableOldName;
            if ( tableName.names.size() == 3 ) { // DatabaseName.SchemaName.TableName
                schemaId = transaction.getCatalog().getSchema( tableName.names.get( 0 ), tableName.names.get( 1 ) ).id;
                tableOldName = tableName.names.get( 2 );
            } else if ( tableName.names.size() == 2 ) { // SchemaName.TableName
                schemaId = transaction.getCatalog().getSchema( context.getDatabaseId(), tableName.names.get( 0 ) ).id;
                tableOldName = tableName.names.get( 1 );
            } else { // TableName
                schemaId = transaction.getCatalog().getSchema( context.getDatabaseId(), context.getDefaultSchemaName() ).id;
                tableOldName = tableName.names.get( 0 );
            }
            catalogTable = transaction.getCatalog().getTable( schemaId, tableOldName );
        } catch ( UnknownDatabaseException e ) {
            throw SqlUtil.newContextException( tableName.getParserPosition(), RESOURCE.databaseNotFound( tableName.toString() ) );
        } catch ( UnknownSchemaException e ) {
            throw SqlUtil.newContextException( tableName.getParserPosition(), RESOURCE.schemaNotFound( tableName.toString() ) );
        } catch ( UnknownTableException e ) {
            throw SqlUtil.newContextException( tableName.getParserPosition(), RESOURCE.tableNotFound( tableName.toString() ) );
        } catch ( UnknownCollationException | UnknownSchemaTypeException | GenericCatalogException e ) {
            throw new RuntimeException( e );
        }
        return catalogTable;
    }


    protected CatalogCombinedTable getCatalogCombinedTable( Context context, Transaction transaction, SqlIdentifier tableName ) {
        try {
            return transaction.getCatalog().getCombinedTable( getCatalogTable( context, transaction, tableName ).id );
        } catch ( GenericCatalogException | UnknownTableException e ) {
            throw new RuntimeException( e );
        }
    }


    protected CatalogColumn getCatalogColumn( Context context, Transaction transaction, long tableId, SqlIdentifier columnName ) {
        CatalogColumn catalogColumn;
        try {
            catalogColumn = transaction.getCatalog().getColumn( tableId, columnName.getSimple() );
        } catch ( GenericCatalogException e ) {
            throw new RuntimeException( e );
        } catch ( UnknownColumnException e ) {
            throw SqlUtil.newContextException( columnName.getParserPosition(), RESOURCE.columnNotFound( columnName.getSimple() ) );
        }
        return catalogColumn;
    }
}
