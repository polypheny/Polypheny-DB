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

package ch.unibas.dmi.dbis.polyphenydb.sql.ddl;


import ch.unibas.dmi.dbis.polyphenydb.jdbc.Context;
import ch.unibas.dmi.dbis.polyphenydb.schema.ColumnStrategy;
import ch.unibas.dmi.dbis.polyphenydb.schema.PolyphenyDbSchema;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlCollation;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDataTypeSpec;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlDrop;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNodeList;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.util.Pair;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
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
    public static SqlCreateTable createTable( SqlParserPos pos, boolean replace, boolean ifNotExists, SqlIdentifier name, SqlNodeList columnList, SqlNode query, SqlIdentifier store ) {
        return new SqlCreateTable( pos, replace, ifNotExists, name, columnList, query, store );
    }


    /**
     * Creates a CREATE VIEW.
     */
    public static SqlCreateView createView( SqlParserPos pos, boolean replace, SqlIdentifier name, SqlNodeList columnList, SqlNode query ) {
        return new SqlCreateView( pos, replace, name, columnList, query );
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
     * Creates a DROP FUNCTION.
     */
    public static SqlDrop dropFunction( SqlParserPos pos, boolean ifExists, SqlIdentifier name ) {
        return new SqlDropFunction( pos, ifExists, name );
    }


    /**
     * Creates a column declaration.
     */
    public static SqlNode column( SqlParserPos pos, SqlIdentifier name, SqlDataTypeSpec dataType, String collation, SqlNode expression, ColumnStrategy strategy ) {
        return new SqlColumnDeclaration( pos, name, dataType, collation, expression, strategy );
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
     * File type for CREATE FUNCTION.
     */
    public enum FileType {
        FILE,
        JAR,
        ARCHIVE
    }
}

