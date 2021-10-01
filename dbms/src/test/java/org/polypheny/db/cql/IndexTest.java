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
 */

package org.polypheny.db.cql;

import org.junit.Assert;
import org.junit.Test;
import org.polypheny.db.cql.exception.UnknownIndexException;
import org.polypheny.db.cql.helper.CqlTestHelper;


public class IndexTest extends CqlTestHelper {

    @Test
    public void testCreateColumnIndex() throws UnknownIndexException {
        ColumnIndex index = ColumnIndex.createIndex( "APP", "test", "testtable", "tbigint" );
        Assert.assertEquals( index.fullyQualifiedName, "test.testtable.tbigint" );
    }


    @Test(expected = UnknownIndexException.class)
    public void testCreateColumnIndexThrowsUnknownIndexException() throws UnknownIndexException {
        ColumnIndex.createIndex( "APP", "hello", "world", "!" );
    }


    @Test
    public void testCreateTableIndex() throws UnknownIndexException {
        TableIndex index = TableIndex.createIndex( "APP", "test", "testtable" );
        Assert.assertEquals( index.fullyQualifiedName, "test.testtable" );
    }


    @Test(expected = UnknownIndexException.class)
    public void testCreateTableIndexThrowsUnknownIndexException() throws UnknownIndexException {
        TableIndex.createIndex( "APP", "hello", "world" );
    }

}
