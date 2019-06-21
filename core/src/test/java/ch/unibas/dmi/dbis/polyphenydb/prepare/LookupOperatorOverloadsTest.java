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
 *  The MIT License (MIT)
 *
 *  Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a
 *  copy of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.prepare;


import static ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunctionCategory.MATCH_RECOGNIZE;
import static ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunctionCategory.USER_DEFINED_CONSTRUCTOR;
import static ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunctionCategory.USER_DEFINED_FUNCTION;
import static ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunctionCategory.USER_DEFINED_PROCEDURE;
import static ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunctionCategory.USER_DEFINED_SPECIFIC_FUNCTION;
import static ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION;
import static ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunctionCategory.USER_DEFINED_TABLE_SPECIFIC_FUNCTION;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import ch.unibas.dmi.dbis.polyphenydb.adapter.java.JavaTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbEmbeddedConnection;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbPrepare.Context;
import ch.unibas.dmi.dbis.polyphenydb.jdbc.PolyphenyDbServerStatement;
import ch.unibas.dmi.dbis.polyphenydb.schema.SchemaPlus;
import ch.unibas.dmi.dbis.polyphenydb.schema.TableFunction;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.AbstractSchema;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.TableFunctionImpl;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlFunctionCategory;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlIdentifier;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperator;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlSyntax;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserPos;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlUserDefinedTableFunction;
import ch.unibas.dmi.dbis.polyphenydb.util.Smalls;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Test;


/**
 * Test for lookupOperatorOverloads() in {@link PolyphenyDbCatalogReader}.
 */
public class LookupOperatorOverloadsTest {

    private void checkFunctionType( int size, String name, List<SqlOperator> operatorList ) {
        assertThat( size, is( operatorList.size() ) );

        for ( SqlOperator op : operatorList ) {
            assertThat( op, instanceOf( SqlUserDefinedTableFunction.class ) );
            assertThat( name, is( op.getName() ) );
        }
    }


    private static void check( List<SqlFunctionCategory> actuals, SqlFunctionCategory... expecteds ) {
        assertThat( actuals, is( Arrays.asList( expecteds ) ) );
    }


    @Test
    public void testIsUserDefined() throws SQLException {
        List<SqlFunctionCategory> cats = new ArrayList<>();
        for ( SqlFunctionCategory c : SqlFunctionCategory.values() ) {
            if ( c.isUserDefined() ) {
                cats.add( c );
            }
        }
        check( cats, USER_DEFINED_FUNCTION, USER_DEFINED_PROCEDURE, USER_DEFINED_CONSTRUCTOR, USER_DEFINED_SPECIFIC_FUNCTION, USER_DEFINED_TABLE_FUNCTION, USER_DEFINED_TABLE_SPECIFIC_FUNCTION );
    }


    @Test
    public void testIsTableFunction() throws SQLException {
        List<SqlFunctionCategory> cats = new ArrayList<>();
        for ( SqlFunctionCategory c : SqlFunctionCategory.values() ) {
            if ( c.isTableFunction() ) {
                cats.add( c );
            }
        }
        check( cats, USER_DEFINED_TABLE_FUNCTION, USER_DEFINED_TABLE_SPECIFIC_FUNCTION, MATCH_RECOGNIZE );
    }


    @Test
    public void testIsSpecific() throws SQLException {
        List<SqlFunctionCategory> cats = new ArrayList<>();
        for ( SqlFunctionCategory c : SqlFunctionCategory.values() ) {
            if ( c.isSpecific() ) {
                cats.add( c );
            }
        }
        check( cats, USER_DEFINED_SPECIFIC_FUNCTION, USER_DEFINED_TABLE_SPECIFIC_FUNCTION );
    }


    @Test
    public void testIsUserDefinedNotSpecificFunction() throws SQLException {
        List<SqlFunctionCategory> cats = new ArrayList<>();
        for ( SqlFunctionCategory sqlFunctionCategory : SqlFunctionCategory.values() ) {
            if ( sqlFunctionCategory.isUserDefinedNotSpecificFunction() ) {
                cats.add( sqlFunctionCategory );
            }
        }
        check( cats, USER_DEFINED_FUNCTION, USER_DEFINED_TABLE_FUNCTION );
    }


    @Test
    public void test() throws SQLException {
        final String schemaName = "MySchema";
        final String funcName = "MyFUNC";
        final String anotherName = "AnotherFunc";

        try ( Connection connection = DriverManager.getConnection( "jdbc:polyphenydbembedded:" ) ) {
            PolyphenyDbEmbeddedConnection polyphenyDbEmbeddedConnection = connection.unwrap( PolyphenyDbEmbeddedConnection.class );
            SchemaPlus rootSchema = polyphenyDbEmbeddedConnection.getRootSchema();
            SchemaPlus schema = rootSchema.add( schemaName, new AbstractSchema() );
            final TableFunction table = TableFunctionImpl.create( Smalls.MAZE_METHOD );
            schema.add( funcName, table );
            schema.add( anotherName, table );
            final TableFunction table2 = TableFunctionImpl.create( Smalls.MAZE3_METHOD );
            schema.add( funcName, table2 );

            final PolyphenyDbServerStatement statement = connection.createStatement().unwrap( PolyphenyDbServerStatement.class );
            final Context prepareContext = statement.createPrepareContext();
            final JavaTypeFactory typeFactory = prepareContext.getTypeFactory();
            PolyphenyDbCatalogReader reader = new PolyphenyDbCatalogReader( prepareContext.getRootSchema(), ImmutableList.of(), typeFactory, prepareContext.config() );

            final List<SqlOperator> operatorList = new ArrayList<>();
            SqlIdentifier myFuncIdentifier = new SqlIdentifier( Lists.newArrayList( schemaName, funcName ), null, SqlParserPos.ZERO, null );
            reader.lookupOperatorOverloads( myFuncIdentifier, SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION, SqlSyntax.FUNCTION, operatorList );
            checkFunctionType( 2, funcName, operatorList );

            operatorList.clear();
            reader.lookupOperatorOverloads( myFuncIdentifier, SqlFunctionCategory.USER_DEFINED_FUNCTION, SqlSyntax.FUNCTION, operatorList );
            checkFunctionType( 0, null, operatorList );

            operatorList.clear();
            SqlIdentifier anotherFuncIdentifier = new SqlIdentifier( Lists.newArrayList( schemaName, anotherName ), null, SqlParserPos.ZERO, null );
            reader.lookupOperatorOverloads( anotherFuncIdentifier, SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION, SqlSyntax.FUNCTION, operatorList );
            checkFunctionType( 1, anotherName, operatorList );
        }
    }
}
