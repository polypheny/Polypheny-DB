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


import ch.unibas.dmi.dbis.polyphenydb.jdbc.Context;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelRoot;
import ch.unibas.dmi.dbis.polyphenydb.schema.ColumnStrategy;
import ch.unibas.dmi.dbis.polyphenydb.schema.PolyphenyDbSchema;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCall;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCollation;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDataTypeSpec;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDrop;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNodeList;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlSelect;
import ch.unibas.dmi.dbis.polyphenydb.sql.dialect.PolyphenyDbSqlDialect;
import ch.unibas.dmi.dbis.polyphenydb.sql.fun.SqlStdOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParseException;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.sql.pretty.SqlPrettyWriter;
import ch.unibas.dmi.dbis.polyphenydb.tools.FrameworkConfig;
import ch.unibas.dmi.dbis.polyphenydb.tools.Frameworks;
import ch.unibas.dmi.dbis.polyphenydb.tools.Planner;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelConversionException;
import ch.unibas.dmi.dbis.polyphenydb.tools.ValidationException;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import com.google.common.collect.ImmutableList;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;


/**
 * Utilities concerning {@link SqlNode} for DDL.
 */
public class SqlDdlNodes {

    private SqlDdlNodes() {
    }


    /**
     * Creates a CREATE SCHEMA.
     */
    public static SqlCreateSchema createSchema( SqlParserPos pos, boolean replace, boolean ifNotExists, SqlIdentifier name ) {
        return new SqlCreateSchema( pos, replace, ifNotExists, name );
    }


    /**
     * Creates a CREATE TYPE.
     */
    public static SqlCreateType createType( SqlParserPos pos, boolean replace, SqlIdentifier name, SqlNodeList attributeList, SqlDataTypeSpec dataTypeSpec ) {
        return new SqlCreateType( pos, replace, name, attributeList, dataTypeSpec );
    }


    /**
     * Creates a CREATE TABLE.
     */
    public static SqlCreateTable createTable( SqlParserPos pos, boolean replace, boolean ifNotExists, SqlIdentifier name, SqlNodeList columnList, SqlNode query ) {
        return new SqlCreateTable( pos, replace, ifNotExists, name, columnList, query );
    }


    /**
     * Creates a CREATE VIEW.
     */
    public static SqlCreateView createView( SqlParserPos pos, boolean replace, SqlIdentifier name, SqlNodeList columnList, SqlNode query ) {
        return new SqlCreateView( pos, replace, name, columnList, query );
    }


    /**
     * Creates a CREATE MATERIALIZED VIEW.
     */
    public static SqlCreateMaterializedView createMaterializedView( SqlParserPos pos, boolean replace, boolean ifNotExists, SqlIdentifier name, SqlNodeList columnList, SqlNode query ) {
        return new SqlCreateMaterializedView( pos, replace, ifNotExists, name, columnList, query );
    }


    /**
     * Creates a CREATE FUNCTION.
     */
    public static SqlCreateFunction createFunction( SqlParserPos pos, boolean replace, boolean ifNotExists, SqlIdentifier name, SqlNode className, SqlNodeList usingList ) {
        return new SqlCreateFunction( pos, replace, ifNotExists, name, className, usingList );
    }


    /**
     * Creates a DROP SCHEMA.
     */
    public static SqlDropSchema dropSchema( SqlParserPos pos, boolean ifExists, SqlIdentifier name ) {
        return new SqlDropSchema( pos, ifExists, name );
    }


    /**
     * Creates a DROP TYPE.
     */
    public static SqlDropType dropType( SqlParserPos pos, boolean ifExists, SqlIdentifier name ) {
        return new SqlDropType( pos, ifExists, name );
    }


    /**
     * Creates a DROP TABLE.
     */
    public static SqlDropTable dropTable( SqlParserPos pos, boolean ifExists, SqlIdentifier name ) {
        return new SqlDropTable( pos, ifExists, name );
    }


    /**
     * Creates a DROP VIEW.
     */
    public static SqlDrop dropView( SqlParserPos pos, boolean ifExists, SqlIdentifier name ) {
        return new SqlDropView( pos, ifExists, name );
    }


    /**
     * Creates a DROP MATERIALIZED VIEW.
     */
    public static SqlDrop dropMaterializedView( SqlParserPos pos, boolean ifExists, SqlIdentifier name ) {
        return new SqlDropMaterializedView( pos, ifExists, name );
    }


    /**
     * Creates a DROP FUNCTION.
     */
    public static SqlDrop dropFunction( SqlParserPos pos, boolean ifExists, SqlIdentifier name ) {
        return new SqlDropFunction( pos, ifExists, name );
    }


    /**
     * Creates a column declaration.
     */
    public static SqlNode column( SqlParserPos pos, SqlIdentifier name, SqlDataTypeSpec dataType, SqlNode expression, ColumnStrategy strategy ) {
        return new SqlColumnDeclaration( pos, name, dataType, expression, strategy );
    }


    /**
     * Creates a attribute definition.
     */
    public static SqlNode attribute( SqlParserPos pos, SqlIdentifier name, SqlDataTypeSpec dataType, SqlNode expression, SqlCollation collation ) {
        return new SqlAttributeDefinition( pos, name, dataType, expression, collation );
    }


    /**
     * Creates a CHECK constraint.
     */
    public static SqlNode check( SqlParserPos pos, SqlIdentifier name, SqlNode expression ) {
        return new SqlCheckConstraint( pos, name, expression );
    }


    /**
     * Creates a UNIQUE constraint.
     */
    public static SqlKeyConstraint unique( SqlParserPos pos, SqlIdentifier name, SqlNodeList columnList ) {
        return new SqlKeyConstraint( pos, name, columnList );
    }


    /**
     * Creates a PRIMARY KEY constraint.
     */
    public static SqlKeyConstraint primary( SqlParserPos pos, SqlIdentifier name, SqlNodeList columnList ) {
        return new SqlKeyConstraint( pos, name, columnList ) {
            @Override
            public SqlOperator getOperator() {
                return PRIMARY;
            }
        };
    }


    /**
     * Returns the schema in which to create an object.
     */
    static Pair<PolyphenyDbSchema, String> schema( Context context, boolean mutable, SqlIdentifier id ) {
        final String name;
        final List<String> path;
        if ( id.isSimple() ) {
            path = context.getDefaultSchemaPath();
            name = id.getSimple();
        } else {
            path = Util.skipLast( id.names );
            name = Util.last( id.names );
        }
        PolyphenyDbSchema schema = context.getRootSchema();
        for ( String p : path ) {
            schema = schema.getSubSchema( p, true );
        }
        return Pair.of( schema, name );
    }


    /**
     * Wraps a query to rename its columns. Used by CREATE VIEW and CREATE MATERIALIZED VIEW.
     */
    static SqlNode renameColumns( SqlNodeList columnList, SqlNode query ) {
        if ( columnList == null ) {
            return query;
        }
        final SqlParserPos p = query.getParserPosition();
        final SqlNodeList selectList = new SqlNodeList( ImmutableList.<SqlNode>of( SqlIdentifier.star( p ) ), p );
        final SqlCall from =
                SqlStdOperatorTable.AS.createCall(
                        p,
                        ImmutableList.<SqlNode>builder()
                                .add( query )
                                .add( new SqlIdentifier( "_", p ) )
                                .addAll( columnList )
                                .build() );
        return new SqlSelect( p, null, selectList, from, null, null, null, null, null, null, null );
    }


    /**
     * Populates the table called {@code name} by executing {@code query}.
     */
    protected static void populate( SqlIdentifier name, SqlNode query, Context context ) {
        // Generate, prepare and execute an "INSERT INTO table query" statement.
        // (It's a bit inefficient that we convert from SqlNode to SQL and back again.)
        final FrameworkConfig config =
                Frameworks.newConfigBuilder()
                        .defaultSchema( context.getRootSchema().plus() )
                        .prepareContext( context )
                        .build();
        final Planner planner = Frameworks.getPlanner( config );
        try {
            final StringWriter sw = new StringWriter();
            final PrintWriter pw = new PrintWriter( sw );
            final SqlPrettyWriter w = new SqlPrettyWriter( PolyphenyDbSqlDialect.DEFAULT, false, pw );
            pw.print( "INSERT INTO " );
            name.unparse( w, 0, 0 );
            pw.print( " " );
            query.unparse( w, 0, 0 );
            pw.flush();
            final String sql = sw.toString();
            final SqlNode query1 = planner.parse( sql );
            final SqlNode query2 = planner.validate( query1 );
            final RelRoot r = planner.rel( query2 );
            final PreparedStatement prepare = context.getRelRunner().prepare( r.rel );
            int rowCount = prepare.executeUpdate();
            Util.discard( rowCount );
            prepare.close();
        } catch ( SqlParseException | ValidationException | RelConversionException | SQLException e ) {
            throw new RuntimeException( e );
        }
    }


    /**
     * File type for CREATE FUNCTION.
     */
    public enum FileType {
        FILE,
        JAR,
        ARCHIVE
    }
}

