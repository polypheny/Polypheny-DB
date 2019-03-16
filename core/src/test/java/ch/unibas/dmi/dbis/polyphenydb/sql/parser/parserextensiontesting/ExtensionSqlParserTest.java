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

package ch.unibas.dmi.dbis.polyphenydb.sql.parser.parserextensiontesting;


import ch.unibas.dmi.dbis.polyphenydb.sql.SqlNode;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParseException;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserImplFactory;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserTest;
import org.hamcrest.core.IsNull;
import org.junit.Test;


/**
 * Testing for extension functionality of the base SQL parser impl.
 *
 * This test runs all test cases of the base {@link SqlParserTest}, as well as verifying specific extension points.
 */
public class ExtensionSqlParserTest extends SqlParserTest {

    @Override
    protected SqlParserImplFactory parserImplFactory() {
        return ExtensionSqlParserImpl.FACTORY;
    }


    @Test
    public void testAlterSystemExtension() throws SqlParseException {
        check( "alter system upload jar '/path/to/jar'", "ALTER SYSTEM UPLOAD JAR '/path/to/jar'" );
    }


    @Test
    public void testAlterSystemExtensionWithoutAlter() throws SqlParseException {
        // We need to include the scope for custom alter operations
        checkFails( "^upload^ jar '/path/to/jar'", "(?s).*Encountered \"upload\" at .*" );
    }


    @Test
    public void testCreateTable() {
        sql( "CREATE TABLE foo.baz(i INTEGER, j VARCHAR(10) NOT NULL)" ).ok( "CREATE TABLE `FOO`.`BAZ` (`I` INTEGER, `J` VARCHAR(10) NOT NULL)" );
    }


    @Test
    public void testExtendedSqlStmt() {
        sql( "DESCRIBE SPACE POWER" ).node( new IsNull<SqlNode>() );
        sql( "DESCRIBE SEA ^POWER^" ).fails( "(?s)Encountered \"POWER\" at line 1, column 14..*" );
    }
}

