/*
 * Copyright 2019-2024 The Polypheny Project
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
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.polypheny.db.sql.language.SqlUtil;
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

        assertFalse( buf.getSql().isEmpty() );
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
                equalTo( """
                        1a2
                        > (they call her "Polythene Pam")
                        3c4,5
                        < She's the kind of a girl that makes The News of The World
                        ---
                        > She's the kind of a girl that makes The Sunday Times
                        > seem more interesting.
                        5d6
                        < Yeah yeah yeah.
                        """ ) );
    }


    @Test
    public void noQuotesSingleStatement() {
        String statement = "SELECT * FROM emp";
        List<String> statements = SqlUtil.splitStatements( statement );
        assertEquals( 1, statements.size() );
        assertEquals( statement, statements.get( 0 ) );
    }


    @Test
    public void doubleQuotesSingleStatement() {
        String statement = "SELECT * FROM \"emp\"";
        List<String> statements = SqlUtil.splitStatements( statement );
        assertEquals( 1, statements.size() );
        assertEquals( statement, statements.get( 0 ) );
    }


    @Test
    public void singleQuotesSingleStatement() {
        String statement = "SELECT 'Hello World' FROM emp";
        List<String> statements = SqlUtil.splitStatements( statement );
        assertEquals( 1, statements.size() );
        assertEquals( statement, statements.get( 0 ) );
    }


    @Test
    public void doubleQuotesEscapeSingleStatement() {
        String statement = "SELECT 'Hello World' FROM \"em\"\"p\"";
        List<String> statements = SqlUtil.splitStatements( statement );
        assertEquals( 1, statements.size() );
        assertEquals( statement, statements.get( 0 ) );
    }


    @Test
    public void doubleQuotesEscapeSingleStatement2() {
        String statement = "SELECT 'Hello World' FROM \"emp\"\"\"";
        List<String> statements = SqlUtil.splitStatements( statement );
        assertEquals( 1, statements.size() );
        assertEquals( statement, statements.get( 0 ) );
    }


    @Test
    public void doubleQuotesEscapeSingleStatement3() {
        String statement = "SELECT 'Hello World' FROM \"\"\"emp\"";
        List<String> statements = SqlUtil.splitStatements( statement );
        assertEquals( 1, statements.size() );
        assertEquals( statement, statements.get( 0 ) );
    }


    @Test
    public void singleQuotesEscapeSingleStatement() {
        String statement = "SELECT 'O''Reilly' FROM emp";
        List<String> statements = SqlUtil.splitStatements( statement );
        assertEquals( 1, statements.size() );
        assertEquals( statement, statements.get( 0 ) );
    }


    @Test
    public void singleQuotesEscapeSingleStatement2() {
        String statement = "SELECT 'Reilly''' FROM emp";
        List<String> statements = SqlUtil.splitStatements( statement );
        assertEquals( 1, statements.size() );
        assertEquals( statement, statements.get( 0 ) );
    }


    @Test
    public void singleQuotesEscapeSingleStatement3() {
        String statement = "SELECT '''OReilly' FROM emp";
        List<String> statements = SqlUtil.splitStatements( statement );
        assertEquals( 1, statements.size() );
        assertEquals( statement, statements.get( 0 ) );
    }


    @Test
    public void unterminatedQuote() {
        Assertions.assertThrows( RuntimeException.class, () -> {
            String statement = "SELECT '";
            SqlUtil.splitStatements( statement );
        } );
    }


    @Test
    public void lineComment() {
        String statement = "-- Only a comment";
        List<String> statements = SqlUtil.splitStatements( statement );
        assertEquals( 0, statements.size() );
    }


    @Test
    public void lineComment2() {
        String statement = "SELECT * FROM emp--comment ";
        List<String> statements = SqlUtil.splitStatements( statement );
        assertEquals( 1, statements.size() );
        assertEquals( "SELECT * FROM emp", statements.get( 0 ) );
    }


    @Test
    public void lineComment3() {
        String statement = "SELECT 1-2-3 FROM emp ";
        List<String> statements = SqlUtil.splitStatements( statement );
        assertEquals( 1, statements.size() );
        assertEquals( "SELECT 1-2-3 FROM emp", statements.get( 0 ) );
    }


    @Test
    public void blockComment() {
        String statement = "/* Only a comment */";
        List<String> statements = SqlUtil.splitStatements( statement );
        assertEquals( 0, statements.size() );
    }


    @Test
    public void blockComment2() {
        String statement = "/**/";
        List<String> statements = SqlUtil.splitStatements( statement );
        assertEquals( 0, statements.size() );
    }


    @Test
    public void blockComment3() {
        String statement = "SELECT 86400 /* 24 * 60 * 60 */";
        List<String> statements = SqlUtil.splitStatements( statement );
        assertEquals( 1, statements.size() );
        assertEquals( "SELECT 86400", statements.get( 0 ) );
    }


    @Test
    public void commentInSingleQuotes() {
        String statement = "SELECT '/* Hello World*/'";
        List<String> statements = SqlUtil.splitStatements( statement );
        assertEquals( 1, statements.size() );
        assertEquals( "SELECT '/* Hello World*/'", statements.get( 0 ) );
    }


    @Test
    public void commentInSingleQuotes2() {
        String statement = "SELECT '-- Hello World'";
        List<String> statements = SqlUtil.splitStatements( statement );
        assertEquals( 1, statements.size() );
        assertEquals( "SELECT '-- Hello World'", statements.get( 0 ) );
    }


    @Test
    public void brackets() {
        String statement = "SELECT (1 + 1) * 2";
        List<String> statements = SqlUtil.splitStatements( statement );
        assertEquals( 1, statements.size() );
        assertEquals( "SELECT (1 + 1) * 2", statements.get( 0 ) );
    }


    @Test
    public void brackets2() {
        String statement = "SELECT timeseries[0]";
        List<String> statements = SqlUtil.splitStatements( statement );
        assertEquals( 1, statements.size() );
        assertEquals( "SELECT timeseries[0]", statements.get( 0 ) );
    }


    @Test
    public void brackets3() {
        String statement = "SELECT {1, 2, 3}";
        List<String> statements = SqlUtil.splitStatements( statement );
        assertEquals( 1, statements.size() );
        assertEquals( "SELECT {1, 2, 3}", statements.get( 0 ) );
    }


    @Test
    public void mixedBrackets() {
        String statement = "SELECT {({1, 2, 3}[0] + timeseries[(3 * t)])}";
        List<String> statements = SqlUtil.splitStatements( statement );
        assertEquals( 1, statements.size() );
        assertEquals( "SELECT {({1, 2, 3}[0] + timeseries[(3 * t)])}", statements.get( 0 ) );
    }


    @Test
    public void unterminatedComment() {
        Assertions.assertThrows( RuntimeException.class, () -> {
            String statement = "SELECT /* Only a comment ";
            SqlUtil.splitStatements( statement );
        } );
    }


    @Test
    public void unterminatedComment2() {
        Assertions.assertThrows( RuntimeException.class, () -> {
            String statement = "SELECT /**";
            SqlUtil.splitStatements( statement );
        } );
    }


    @Test
    public void unbalancedBrackets() {
        Assertions.assertThrows( RuntimeException.class, () -> {
            String statement = "SELECT (";
            SqlUtil.splitStatements( statement );
        } );
    }


    @Test
    public void unbalancedBrackets2() {
        Assertions.assertThrows( RuntimeException.class, () -> {
            String statement = "SELECT ([)";
            SqlUtil.splitStatements( statement );
        } );
    }


    @Test
    public void unbalancedBrackets3() {
        Assertions.assertThrows( RuntimeException.class, () -> {
            String statement = "SELECT (3 * 4;)";
            SqlUtil.splitStatements( statement );
        } );
    }


    @Test
    public void multipleStatements() {
        String statement = "SELECT * FROM emp;"
                + "SELECT * FROM emp;";
        List<String> statements = SqlUtil.splitStatements( statement );
        assertEquals( 2, statements.size() );
        assertEquals( "SELECT * FROM emp", statements.get( 0 ) );
        assertEquals( "SELECT * FROM emp", statements.get( 1 ) );

    }


    @Test
    public void multipleStatements2() {
        String statement = """
                SEL/**/ECT "id/*""\", 'O''Reily--' FROM emp; -- Comment


                COMMIT;
                /**/SEL--
                ECT "", username FROM emp--""";
        List<String> statements = SqlUtil.splitStatements( statement );
        assertEquals( 3, statements.size() );
        assertEquals( "SEL ECT \"id/*\"\"\", 'O''Reily--' FROM emp", statements.get( 0 ) );
        assertEquals( "COMMIT", statements.get( 1 ) );
        assertEquals( "SEL ECT \"\", username FROM emp", statements.get( 2 ) );
    }


    // These are for full branch coverage
    @Test
    public void invalidSql() {
        String statement = "SELECT 1 /";
        List<String> statements = SqlUtil.splitStatements( statement );
        assertEquals( 1, statements.size() );
        assertEquals( "SELECT 1 /", statements.get( 0 ) );
    }


    @Test
    public void invalidSql2() {
        String statement = "SELECT 1 / ";
        List<String> statements = SqlUtil.splitStatements( statement );
        assertEquals( 1, statements.size() );
        assertEquals( "SELECT 1 /", statements.get( 0 ) );
    }


    @Test
    public void invalidSql3() {
        String statement = "SELECT 1 -";
        List<String> statements = SqlUtil.splitStatements( statement );
        assertEquals( 1, statements.size() );
        assertEquals( "SELECT 1 -", statements.get( 0 ) );
    }

}
