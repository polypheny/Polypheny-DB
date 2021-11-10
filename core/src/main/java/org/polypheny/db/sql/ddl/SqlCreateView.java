/*
 * Copyright 2019-2021 The Polypheny Project
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

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import lombok.Getter;
import org.apache.calcite.linq4j.Ord;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.catalog.Catalog.PlacementType;
import org.polypheny.db.catalog.exceptions.GenericCatalogException;
import org.polypheny.db.catalog.exceptions.TableAlreadyExistsException;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.catalog.exceptions.UnknownDatabaseException;
import org.polypheny.db.catalog.exceptions.UnknownSchemaException;
import org.polypheny.db.config.RuntimeConfig;
import org.polypheny.db.core.ParserPos;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.jdbc.Context;
import org.polypheny.db.processing.SqlProcessor;
import org.polypheny.db.rel.RelCollation;
import org.polypheny.db.rel.RelNode;
import org.polypheny.db.rel.RelRoot;
import org.polypheny.db.sql.SqlCreate;
import org.polypheny.db.sql.SqlExecutableStatement;
import org.polypheny.db.sql.SqlIdentifier;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlNode;
import org.polypheny.db.sql.SqlNodeList;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.sql.SqlSpecialOperator;
import org.polypheny.db.sql.SqlUtil;
import org.polypheny.db.sql.SqlWriter;
import org.polypheny.db.sql.dialect.PolyphenyDbSqlDialect;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.util.ImmutableNullableList;


/**
 * Parse tree for {@code CREATE VIEW} statement.
 */
public class SqlCreateView extends SqlCreate implements SqlExecutableStatement {

    private final SqlIdentifier name;
    private final SqlNodeList columnList;
    @Getter
    private final SqlNode query;

    private static final SqlOperator OPERATOR = new SqlSpecialOperator( "CREATE VIEW", SqlKind.CREATE_VIEW );


    /**
     * Creates a SqlCreateView.
     */
    SqlCreateView(
            ParserPos pos,
            boolean replace,
            SqlIdentifier name,
            SqlNodeList columnList,
            SqlNode query ) {
        super( OPERATOR, pos, replace, false );
        this.name = Objects.requireNonNull( name );
        this.columnList = columnList; // may be null
        this.query = Objects.requireNonNull( query );
    }


    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of( name, columnList, query );
    }


    @Override
    public void execute( Context context, Statement statement ) {
        Catalog catalog = Catalog.getInstance();
        String viewName;
        long schemaId;

        try {
            if ( name.names.size() == 3 ) { // DatabaseName.SchemaName.TableName
                schemaId = catalog.getSchema( name.names.get( 0 ), name.names.get( 1 ) ).id;
                viewName = name.names.get( 2 );
            } else if ( name.names.size() == 2 ) { // SchemaName.TableName
                schemaId = catalog.getSchema( context.getDatabaseId(), name.names.get( 0 ) ).id;
                viewName = name.names.get( 1 );
            } else { // TableName
                schemaId = catalog.getSchema( context.getDatabaseId(), context.getDefaultSchemaName() ).id;
                viewName = name.names.get( 0 );
            }
        } catch ( UnknownDatabaseException e ) {
            throw SqlUtil.newContextException( name.getPos(), RESOURCE.databaseNotFound( name.toString() ) );
        } catch ( UnknownSchemaException e ) {
            throw SqlUtil.newContextException( name.getPos(), RESOURCE.schemaNotFound( name.toString() ) );
        }

        PlacementType placementType = PlacementType.AUTOMATIC;

        SqlProcessor sqlProcessor = statement.getTransaction().getSqlProcessor();
        RelRoot relRoot = sqlProcessor.translate(
                statement,
                sqlProcessor.validate( statement.getTransaction(), this.query, RuntimeConfig.ADD_DEFAULT_VALUES_IN_INSERTS.getBoolean() ).left );

        RelNode relNode = relRoot.rel;
        RelCollation relCollation = relRoot.collation;

        List<String> columns = null;

        if ( columnList != null ) {
            columns = getColumnInfo();
        }

        try {
            DdlManager.getInstance().createView(
                    viewName,
                    schemaId,
                    relNode,
                    relCollation,
                    replace,
                    statement,
                    placementType,
                    columns,
                    String.valueOf( query.toSqlString( PolyphenyDbSqlDialect.DEFAULT ) ),
                    Catalog.QueryLanguage.SQL );
        } catch ( TableAlreadyExistsException e ) {
            throw SqlUtil.newContextException( name.getPos(), RESOURCE.tableExists( viewName ) );
        } catch ( GenericCatalogException | UnknownColumnException e ) {
            // we just added the table/column so it has to exist or we have a internal problem
            throw new RuntimeException( e );
        }
    }


    private List<String> getColumnInfo() {
        List<String> columnName = new ArrayList<>();

        for ( Ord<SqlNode> c : Ord.zip( columnList ) ) {
            if ( c.e instanceof SqlIdentifier ) {
                SqlIdentifier sqlIdentifier = (SqlIdentifier) c.e;
                columnName.add( sqlIdentifier.getSimple() );

            } else {
                throw new AssertionError( c.e.getClass() );
            }
        }
        return columnName;
    }


    @Override
    public void unparse( SqlWriter writer, int leftPrec, int rightPrec ) {
        if ( getReplace() ) {
            writer.keyword( "CREATE OR REPLACE" );
        } else {
            writer.keyword( "CREATE" );
        }
        writer.keyword( "VIEW" );
        name.unparse( writer, leftPrec, rightPrec );
        if ( columnList != null ) {
            SqlWriter.Frame frame = writer.startList( "(", ")" );
            for ( SqlNode c : columnList ) {
                writer.sep( "," );
                c.unparse( writer, 0, 0 );
            }
            writer.endList( frame );
        }
        writer.keyword( "AS" );
        writer.newlineAndIndent();
        query.unparse( writer, 0, 0 );
    }


    /*
    public void execute( Context context ) {
        final Pair<PolyphenyDbSchema, String> pair = SqlDdlNodes.schema( context, true, name );
        final SchemaPlus schemaPlus = pair.left.plus();
        for ( Function function : schemaPlus.getFunctions( pair.right ) ) {
            if ( function.getParameters().isEmpty() ) {
                if ( !getReplace() ) {
                    throw SqlUtil.newContextException( name.getParserPosition(), RESOURCE.viewExists( pair.right ) );
                }
                pair.left.removeFunction( pair.right );
            }
        }
        final SqlNode q = SqlDdlNodes.renameColumns( columnList, query );
        final String sql = q.toSqlString( PolyphenyDbSqlDialect.DEFAULT ).getSql();
        final ViewTableMacro viewTableMacro = ViewTable.viewMacro( schemaPlus, sql, pair.left.path( null ), context.getObjectPath(), false );
        final TranslatableTable x = viewTableMacro.apply( ImmutableList.of() );
        Util.discard( x );
        schemaPlus.add( pair.right, viewTableMacro );
    } */

}

