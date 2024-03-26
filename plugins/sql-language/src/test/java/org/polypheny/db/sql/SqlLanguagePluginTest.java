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

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.processing.QueryContext;
import org.polypheny.db.processing.QueryContext.ParsedQueryContext;
import org.polypheny.db.transaction.TransactionManager;

public class SqlLanguagePluginTest extends SqlLanguageDependent {

    TransactionManager transactionManager;
    QueryLanguage sql;


    @BeforeEach
    public void init() {
        transactionManager = testHelper.getTransactionManager();
        sql = QueryLanguage.from( "sql" );
    }


    public QueryContext getContext( String query ) {
        return QueryContext.builder()
                .query( query )
                .language( sql )
                .origin( this.getClass().toString() )
                .transactionManager( transactionManager )
                .build();
    }


    @Test
    public void testQueryWithSemicolon() {
        QueryContext context = getContext( "SELECT * FROM employee WHERE ename = ';'" );

        List<ParsedQueryContext> res = sql.splitter().apply( context );
        assertEquals( 1, res.size() );
    }


    @Test
    public void testTwoQueries() {
        QueryContext context = getContext( "SELECT * FROM employee WHERE ename = 'a'; SELECT * FROM employee WHERE ename = 'b'" );

        List<ParsedQueryContext> res = sql.splitter().apply( context );
        assertEquals( 2, res.size() );
    }


    @Test
    public void testTwoQueriesWithSemicolon() {
        QueryContext context = getContext( "SELECT * FROM employee WHERE ename = ';'; SELECT * FROM employee WHERE ename = ';'" );

        List<ParsedQueryContext> res = sql.splitter().apply( context );
        assertEquals( 2, res.size() );
    }


    @Test
    public void testQueryWithLimit() {
        String query = "SELECT * FROM employee WHERE ename = 'limit';";

        QueryContext context = getContext( query );
        QueryContext res = QueryLanguage.from( "sql" ).limitRemover().apply( context );

        assertEquals( query, res.getQuery() );
        assertEquals( context.getBatch(), res.getBatch() );
    }


    @Test
    public void testQueryWithTwoLimits() {
        String query = "SELECT * FROM employee WHERE ename = 'limit 5' LIMIT 10;";
        QueryContext context = getContext( query );

        QueryContext res = QueryLanguage.from( "sql" ).limitRemover().apply( context );
        assertEquals( 10, res.getBatch() );
    }

}
