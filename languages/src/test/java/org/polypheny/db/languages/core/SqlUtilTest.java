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

package org.polypheny.db.languages.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Timestamp;
import org.junit.Test;
import org.polypheny.db.languages.sql.dialect.PolyphenyDbSqlDialect;
import org.polypheny.db.languages.sql.util.SqlBuilder;
import org.polypheny.db.languages.sql.util.SqlString;

public class SqlUtilTest {

    /**
     * Tests SQL builders.
     */
    @Test
    public static void testSqlBuilder() {
        final SqlBuilder buf = new SqlBuilder( PolyphenyDbSqlDialect.DEFAULT );
        assertEquals( 0, buf.length() );
        buf.append( "select " );
        assertEquals( "select ", buf.getSql() );

        buf.identifier( "x" );
        assertEquals( "select \"x\"", buf.getSql() );

        buf.append( ", " );
        buf.identifier( "y", "a b" );
        assertEquals( "select \"x\", \"y\".\"a b\"", buf.getSql() );

        final SqlString sqlString = buf.toSqlString();
        assertEquals( PolyphenyDbSqlDialect.DEFAULT, sqlString.getDialect() );
        assertEquals( buf.getSql(), sqlString.getSql() );

        assertTrue( buf.getSql().length() > 0 );
        assertEquals( buf.getSqlAndClear(), sqlString.getSql() );
        assertEquals( 0, buf.length() );

        buf.clear();
        assertEquals( 0, buf.length() );

        buf.literal( "can't get no satisfaction" );
        assertEquals( "'can''t get no satisfaction'", buf.getSqlAndClear() );

        buf.literal( new Timestamp( 0 ) );
        assertEquals( "TIMESTAMP '1970-01-01 00:00:00'", buf.getSqlAndClear() );

        buf.clear();
        assertEquals( 0, buf.length() );

        buf.append( "hello world" );
        assertEquals( 2, buf.indexOf( "l" ) );
        assertEquals( -1, buf.indexOf( "z" ) );
        assertEquals( 9, buf.indexOf( "l", 5 ) );
    }

}
