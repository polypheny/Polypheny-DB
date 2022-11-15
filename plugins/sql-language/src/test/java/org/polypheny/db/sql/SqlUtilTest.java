/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.sql;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.sql.Timestamp;
import java.util.Arrays;
import org.junit.Test;
import org.polypheny.db.sql.language.dialect.PolyphenyDbSqlDialect;
import org.polypheny.db.sql.language.util.SqlBuilder;
import org.polypheny.db.sql.language.util.SqlString;
import org.polypheny.db.util.Util;

public class SqlUtilTest {

    /**
     * Tests SQL builders.
     */
    @Test
    public void testSqlBuilder() {
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


    /**
     * Tests the difference engine, {@link DiffTestCase#diff}.
     */
    @Test
    public void testDiffLines() {
        String[] before = {
                "Get a dose of her in jackboots and kilt",
                "She's killer-diller when she's dressed to the hilt",
                "She's the kind of a girl that makes The News of The World",
                "Yes you could say she was attractively built.",
                "Yeah yeah yeah."
        };
        String[] after = {
                "Get a dose of her in jackboots and kilt",
                "(they call her \"Polythene Pam\")",
                "She's killer-diller when she's dressed to the hilt",
                "She's the kind of a girl that makes The Sunday Times",
                "seem more interesting.",
                "Yes you could say she was attractively built."
        };
        String diff = DiffTestCase.diffLines( Arrays.asList( before ), Arrays.asList( after ) );
        assertThat(
                Util.toLinux( diff ),
                equalTo( "1a2\n"
                        + "> (they call her \"Polythene Pam\")\n"
                        + "3c4,5\n"
                        + "< She's the kind of a girl that makes The News of The World\n"
                        + "---\n"
                        + "> She's the kind of a girl that makes The Sunday Times\n"
                        + "> seem more interesting.\n"
                        + "5d6\n"
                        + "< Yeah yeah yeah.\n" ) );
    }

}
