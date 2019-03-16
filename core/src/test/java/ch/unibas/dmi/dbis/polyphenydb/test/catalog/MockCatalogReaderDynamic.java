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

package ch.unibas.dmi.dbis.polyphenydb.test.catalog;


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.sql.type.SqlTypeName;


/**
 * Registers dynamic tables.
 *
 * Not thread-safe.
 */
public class MockCatalogReaderDynamic extends MockCatalogReader {

    /**
     * Creates a MockCatalogReader.
     *
     * Caller must then call {@link #init} to populate with data.
     *
     * @param typeFactory Type factory
     * @param caseSensitive case sensitivity
     */
    public MockCatalogReaderDynamic( RelDataTypeFactory typeFactory, boolean caseSensitive ) {
        super( typeFactory, caseSensitive );
    }


    @Override
    public MockCatalogReader init() {
        // Register "DYNAMIC" schema.
        MockSchema schema = new MockSchema( "SALES" );
        registerSchema( schema );

        MockTable nationTable = new MockDynamicTable( this, schema.getCatalogName(), schema.getName(), "NATION", false, 100 );
        registerTable( nationTable );

        MockTable customerTable = new MockDynamicTable( this, schema.getCatalogName(), schema.getName(), "CUSTOMER", false, 100 );
        registerTable( customerTable );

        // CREATE TABLE "REGION" - static table with known schema.
        final RelDataType intType = typeFactory.createSqlType( SqlTypeName.INTEGER );
        final RelDataType varcharType = typeFactory.createSqlType( SqlTypeName.VARCHAR );

        MockTable regionTable = MockTable.create( this, schema, "REGION", false, 100 );
        regionTable.addColumn( "R_REGIONKEY", intType );
        regionTable.addColumn( "R_NAME", varcharType );
        regionTable.addColumn( "R_COMMENT", varcharType );
        registerTable( regionTable );

        return this;
    }
}

