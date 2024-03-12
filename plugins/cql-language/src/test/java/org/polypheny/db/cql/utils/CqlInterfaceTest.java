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

package org.polypheny.db.cql.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.google.gson.JsonObject;
import java.sql.Date;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import kong.unirest.HttpRequestWithBody;
import kong.unirest.HttpResponse;
import kong.unirest.JsonNode;
import kong.unirest.Unirest;
import kong.unirest.json.JSONArray;
import kong.unirest.json.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.cql.utils.helper.CqlTestHelper;


@Slf4j
public class CqlInterfaceTest extends CqlTestHelper {

    private static TestHelper helper;


    @BeforeAll
    public static void setUp() {
        helper = TestHelper.getInstance();

    }


    @Test
    public void testRestCqlEmptyQueryReturnsException() {
        String error = "CQL query is empty";
        containsError( "", error );
    }


    private void containsError( String cqlQuery, String error ) {
        HttpResponse<JsonNode> response = executeCQL( cqlQuery );
        JSONObject object = response.getBody().getArray().getJSONObject( 0 );
        assertTrue( object.has( "error" ) );
        String res = object.getString( "error" );
        assertFalse( res.isEmpty() );
        assertTrue( res.equalsIgnoreCase( error ) );
    }


    @Test
    public void testRestCqlFilterOnlyQuery() {
        JsonNode expectedJsonNode = new JsonNode( "{\"result\":[{\"test.employee.married\":false,"
                + "\"test.employee.empname\":\"Anagha\",\"test.employee.empno\":18,"
                + "\"test.employee.joining_date\":\"2021-10-14\",\"test.employee.dob\":\"2000-10-14\","
                + "\"test.employee.salary\":90000.0,\"test.employee.deptno\":5}],\"size\":1}" );

        cqlInterfaceTestHelper( "test.employee.empname == \"Anagha\"", expectedJsonNode, true );
    }


    @Test
    public void testRestCqlFiltersOnlyQueryWithANDOperator() {
        JsonNode expectedJsonNode = new JsonNode( "{\"result\":[{\"test.employee.married\":true,"
                + "\"test.employee.empname\":\"Holt\",\"test.dept.deptno\":6,\"test.employee.empno\":21,"
                + "\"test.employee.joining_date\":\"1990-01-01\",\"test.employee.dob\":\"1958-02-25\",\"test.employee.salary\":87000.0,"
                + "\"test.employee.deptno\":6,\"test.dept.deptname\":\"IT\"},{\"test.employee.married\":true,"
                + "\"test.employee.empname\":\"Mando\",\"test.dept.deptno\":6,\"test.employee.empno\":23,"
                + "\"test.employee.joining_date\":\"1990-01-01\",\"test.employee.dob\":\"1967-11-30\",\"test.employee.salary\":35700.0,"
                + "\"test.employee.deptno\":6,\"test.dept.deptname\":\"IT\"}],\"size\":2}" );

        cqlInterfaceTestHelper( "test.dept.deptname = \"IT\" and test.employee.married = TRUE", expectedJsonNode, true );
    }


    @Test
    public void testRestCqlFiltersOnlyQueryWithOROperator() {
        JsonNode expectedJsonNode = new JsonNode( "{\"result\":[{\"test.dept.deptno\":1,"
                + "\"test.dept.deptname\":\"Human Resources\"},{\"test.dept.deptno\":6,"
                + "\"test.dept.deptname\":\"IT\"}],\"size\":2}" );

        cqlInterfaceTestHelper( "test.dept.deptname = \"IT\" or test.dept.deptname = \"Human Resources\"", expectedJsonNode, true );
    }


    @Test
    public void testRestCqlFiltersOnlyQueryWithNOTOperator() {
        JsonNode expectedJsonNode = new JsonNode( """
                {"result":[
                    {"test.employee.married":true,"test.employee.empname":"Imane","test.dept.deptno":2,"test.employee.empno":5,
                    "test.employee.joining_date":"2001-11-01","test.employee.dob":"1989-03-03","test.employee.salary":27000.0,
                    "test.employee.deptno":2,"test.dept.deptname":"Marketing"},
                   \s
                    {"test.employee.married":false,"test.employee.empname":"Rhody","test.dept.deptno":2,"test.employee.empno":6,
                    "test.employee.joining_date":"1996-11-12","test.employee.dob":"1970-07-21","test.employee.salary":67500.0,
                    "test.employee.deptno":2,"test.dept.deptname":"Marketing"},
                   \s
                    {"test.employee.married":true,"test.employee.empname":"Cryer","test.dept.deptno":2,"test.employee.empno":7,
                    "test.employee.joining_date":"1990-01-01","test.employee.dob":"1969-12-31","test.employee.salary":17000.0,
                    "test.employee.deptno":2,"test.dept.deptname":"Marketing"},
                   \s
                    {"test.employee.married":false,"test.employee.empname":"Lily","test.dept.deptno":2,"test.employee.empno":8,
                    "test.employee.joining_date":"2002-03-01","test.employee.dob":"1980-02-03","test.employee.salary":34000.0,
                    "test.employee.deptno":2,"test.dept.deptname":"Marketing"},
                   \s
                    {"test.employee.married":true,"test.employee.empname":"Rose","test.dept.deptno":3,"test.employee.empno":9,
                    "test.employee.joining_date":"2008-01-12","test.employee.dob":"1983-06-13","test.employee.salary":15000.0,
                    "test.employee.deptno":3,"test.dept.deptname":"Production"},
                   \s
                    {"test.employee.married":false,"test.employee.empname":"Happy","test.dept.deptno":3,"test.employee.empno":10,
                    "test.employee.joining_date":"1990-09-16","test.employee.dob":"1965-09-16","test.employee.salary":19000.0,
                    "test.employee.deptno":3,"test.dept.deptname":"Production"},
                   \s
                    {"test.employee.married":true,"test.employee.empname":"Marcus","test.dept.deptno":3,"test.employee.empno":11,
                    "test.employee.joining_date":"1990-01-15","test.employee.dob":"1966-12-12","test.employee.salary":80000.0,
                    "test.employee.deptno":3,"test.dept.deptname":"Production"},
                   \s
                    {"test.employee.married":false,"test.employee.empname":"Mary","test.dept.deptno":3,"test.employee.empno":12,
                    "test.employee.joining_date":"1970-04-23","test.employee.dob":"1950-12-31","test.employee.salary":60000.0,
                    "test.employee.deptno":3,"test.dept.deptname":"Production"},
                   \s
                    {"test.employee.married":true,"test.employee.empname":"Joy","test.dept.deptno":4,"test.employee.empno":13,
                    "test.employee.joining_date":"2006-04-01","test.employee.dob":"1979-03-19","test.employee.salary":65000.0,
                    "test.employee.deptno":4,"test.dept.deptname":"Research and Development"},
                   \s
                    {"test.employee.married":false,"test.employee.empname":"Debby","test.dept.deptno":4,"test.employee.empno":14,
                    "test.employee.joining_date":"1999-01-01","test.employee.dob":"1969-04-30","test.employee.salary":50000.0,
                    "test.employee.deptno":4,"test.dept.deptname":"Research and Development"},
                   \s
                    {"test.employee.married":true,"test.employee.empname":"Troy","test.dept.deptno":4,"test.employee.empno":15,
                    "test.employee.joining_date":"2021-08-16","test.employee.dob":"1999-05-01","test.employee.salary":55000.0,
                    "test.employee.deptno":4,"test.dept.deptname":"Research and Development"},
                   \s
                    {"test.employee.married":false,"test.employee.empname":"Roy","test.dept.deptno":4,"test.employee.empno":16,
                    "test.employee.joining_date":"1991-03-12","test.employee.dob":"1966-02-28","test.employee.salary":57000.0,
                    "test.employee.deptno":4,"test.dept.deptname":"Research and Development"},
                   \s
                    {"test.employee.married":true,"test.employee.empname":"Rich","test.dept.deptno":5,"test.employee.empno":17,
                    "test.employee.joining_date":"2019-04-01","test.employee.dob":"1999-11-23","test.employee.salary":45000.0,
                    "test.employee.deptno":5,"test.dept.deptname":"Accounting and Finance"},
                   \s
                    {"test.employee.married":false,"test.employee.empname":"Anagha","test.dept.deptno":5,"test.employee.empno":18,
                    "test.employee.joining_date":"2021-10-14","test.employee.dob":"2000-10-14","test.employee.salary":90000.0,
                    "test.employee.deptno":5,"test.dept.deptname":"Accounting and Finance"},
                   \s
                    {"test.employee.married":true,"test.employee.empname":"Diana","test.dept.deptno":5,"test.employee.empno":19,
                    "test.employee.joining_date":"1970-06-03","test.employee.dob":"1945-01-03","test.employee.salary":19700.0,
                    "test.employee.deptno":5,"test.dept.deptname":"Accounting and Finance"},
                   \s
                    {"test.employee.married":false,"test.employee.empname":"Matt","test.dept.deptno":5,"test.employee.empno":20,
                    "test.employee.joining_date":"1989-06-16","test.employee.dob":"1956-12-13","test.employee.salary":33000.0,
                    "test.employee.deptno":5,"test.dept.deptname":"Accounting and Finance"},
                   \s
                    {"test.employee.married":true,"test.employee.empname":"Holt","test.dept.deptno":6,"test.employee.empno":21,
                    "test.employee.joining_date":"1990-01-01","test.employee.dob":"1958-02-25","test.employee.salary":87000.0,
                    "test.employee.deptno":6,"test.dept.deptname":"IT"},
                   \s
                    {"test.employee.married":false,"test.employee.empname":"Peralta","test.dept.deptno":6,"test.employee.empno":22,
                    "test.employee.joining_date":"1990-01-01","test.employee.dob":"1980-01-31","test.employee.salary":22000.0,
                    "test.employee.deptno":6,"test.dept.deptname":"IT"},
                   \s
                    {"test.employee.married":true,"test.employee.empname":"Mando","test.dept.deptno":6,"test.employee.empno":23,
                    "test.employee.joining_date":"1990-01-01","test.employee.dob":"1967-11-30","test.employee.salary":35700.0,
                    "test.employee.deptno":6,"test.dept.deptname":"IT"},
                   \s
                    {"test.employee.married":false,"test.employee.empname":"Vader","test.dept.deptno":6,"test.employee.empno":24,
                    "test.employee.joining_date":"1990-01-01","test.employee.dob":"1959-12-13","test.employee.salary":3000.0,
                    "test.employee.deptno":6,"test.dept.deptname":"IT"}],
                   \s
                    "size":20}
                """ );

        cqlInterfaceTestHelper( "test.employee.empno >= 1 NOT test.dept.deptname = \"Human Resources\"", expectedJsonNode, false );
    }


    @Test
    public void testRestCqlFiltersOnlyQueryWithPROXOperator() {
        String error = "'PROX' boolean operator not implemented.";
        containsError( "test.dept.deptname = \"IT\" PROX test.dept.deptname = \"Human Resources\"", error );
    }


    @Test
    public void testRestCqlRelationOnlyQuery() {
        JsonNode expectedJsonNode = new JsonNode( "{\"result\":[{\"test.dept.deptno\":1,"
                + "\"test.dept.deptname\":\"Human Resources\"},{\"test.dept.deptno\":2,"
                + "\"test.dept.deptname\":\"Marketing\"},{\"test.dept.deptno\":3,"
                + "\"test.dept.deptname\":\"Production\"},{\"test.dept.deptno\":4,"
                + "\"test.dept.deptname\":\"Research and Development\"},{\"test.dept.deptno\":5,"
                + "\"test.dept.deptname\":\"Accounting and Finance\"},{\"test.dept.deptno\":6,"
                + "\"test.dept.deptname\":\"IT\"}],\"size\":6}" );

        cqlInterfaceTestHelper( "relation test.dept", expectedJsonNode, true );
    }


    @Test
    public void testRestCqlRelationOnlyQueryWithANDOperator() {
        JsonNode expectedJsonNode = new JsonNode( """
                {"result":[
                    {"test.employee.married":true,"test.employee.empname":"Joe","test.dept.deptno":1,"test.employee.empno":1,
                    "test.employee.joining_date":"1990-06-01","test.employee.dob":"1970-02-03","test.employee.salary":10000.0,
                    "test.employee.deptno":1,"test.dept.deptname":"Human Resources"},
                   \s
                    {"test.employee.married":false,"test.employee.empname":"Amy","test.dept.deptno":1,"test.employee.empno":2,
                    "test.employee.joining_date":"2001-03-07","test.employee.dob":"1977-06-03","test.employee.salary":25000.0,
                    "test.employee.deptno":1,"test.dept.deptname":"Human Resources"},
                   \s
                    {"test.employee.married":true,"test.employee.empname":"Charlie","test.dept.deptno":1,"test.employee.empno":3,
                    "test.employee.joining_date":"2000-02-18","test.employee.dob":"1980-04-12","test.employee.salary":16000.0,
                    "test.employee.deptno":1,"test.dept.deptname":"Human Resources"},
                   \s
                    {"test.employee.married":false,"test.employee.empname":"Ravi","test.dept.deptno":1,"test.employee.empno":4,
                    "test.employee.joining_date":"1990-12-17","test.employee.dob":"1970-11-11","test.employee.salary":40000.0,
                    "test.employee.deptno":1,"test.dept.deptname":"Human Resources"},
                   \s
                    {"test.employee.married":true,"test.employee.empname":"Imane","test.dept.deptno":2,"test.employee.empno":5,
                    "test.employee.joining_date":"2001-11-01","test.employee.dob":"1989-03-03","test.employee.salary":27000.0,
                    "test.employee.deptno":2,"test.dept.deptname":"Marketing"},
                   \s
                    {"test.employee.married":false,"test.employee.empname":"Rhody","test.dept.deptno":2,"test.employee.empno":6,
                    "test.employee.joining_date":"1996-11-12","test.employee.dob":"1970-07-21","test.employee.salary":67500.0,
                    "test.employee.deptno":2,"test.dept.deptname":"Marketing"},
                   \s
                    {"test.employee.married":true,"test.employee.empname":"Cryer","test.dept.deptno":2,"test.employee.empno":7,
                    "test.employee.joining_date":"1990-01-01","test.employee.dob":"1969-12-31","test.employee.salary":17000.0,
                    "test.employee.deptno":2,"test.dept.deptname":"Marketing"},
                   \s
                    {"test.employee.married":false,"test.employee.empname":"Lily","test.dept.deptno":2,"test.employee.empno":8,
                    "test.employee.joining_date":"2002-03-01","test.employee.dob":"1980-02-03","test.employee.salary":34000.0,
                    "test.employee.deptno":2,"test.dept.deptname":"Marketing"},
                   \s
                    {"test.employee.married":true,"test.employee.empname":"Rose","test.dept.deptno":3,"test.employee.empno":9,
                    "test.employee.joining_date":"2008-01-12","test.employee.dob":"1983-06-13","test.employee.salary":15000.0,
                    "test.employee.deptno":3,"test.dept.deptname":"Production"},
                   \s
                    {"test.employee.married":false,"test.employee.empname":"Happy","test.dept.deptno":3,"test.employee.empno":10,
                    "test.employee.joining_date":"1990-09-16","test.employee.dob":"1965-09-16","test.employee.salary":19000.0,
                    "test.employee.deptno":3,"test.dept.deptname":"Production"},
                   \s
                    {"test.employee.married":true,"test.employee.empname":"Marcus","test.dept.deptno":3,"test.employee.empno":11,
                    "test.employee.joining_date":"1990-01-15","test.employee.dob":"1966-12-12","test.employee.salary":80000.0,
                    "test.employee.deptno":3,"test.dept.deptname":"Production"},
                   \s
                    {"test.employee.married":false,"test.employee.empname":"Mary","test.dept.deptno":3,"test.employee.empno":12,
                    "test.employee.joining_date":"1970-04-23","test.employee.dob":"1950-12-31","test.employee.salary":60000.0,
                    "test.employee.deptno":3,"test.dept.deptname":"Production"},
                   \s
                    {"test.employee.married":true,"test.employee.empname":"Joy","test.dept.deptno":4,"test.employee.empno":13,
                    "test.employee.joining_date":"2006-04-01","test.employee.dob":"1979-03-19","test.employee.salary":65000.0,
                    "test.employee.deptno":4,"test.dept.deptname":"Research and Development"},
                   \s
                    {"test.employee.married":false,"test.employee.empname":"Debby","test.dept.deptno":4,"test.employee.empno":14,
                    "test.employee.joining_date":"1999-01-01","test.employee.dob":"1969-04-30","test.employee.salary":50000.0,
                    "test.employee.deptno":4,"test.dept.deptname":"Research and Development"},
                   \s
                    {"test.employee.married":true,"test.employee.empname":"Troy","test.dept.deptno":4,"test.employee.empno":15,
                    "test.employee.joining_date":"2021-08-16","test.employee.dob":"1999-05-01","test.employee.salary":55000.0,
                    "test.employee.deptno":4,"test.dept.deptname":"Research and Development"},
                   \s
                    {"test.employee.married":false,"test.employee.empname":"Roy","test.dept.deptno":4,"test.employee.empno":16,
                    "test.employee.joining_date":"1991-03-12","test.employee.dob":"1966-02-28","test.employee.salary":57000.0,
                    "test.employee.deptno":4,"test.dept.deptname":"Research and Development"},
                   \s
                    {"test.employee.married":true,"test.employee.empname":"Rich","test.dept.deptno":5,"test.employee.empno":17,
                    "test.employee.joining_date":"2019-04-01","test.employee.dob":"1999-11-23","test.employee.salary":45000.0,
                    "test.employee.deptno":5,"test.dept.deptname":"Accounting and Finance"},
                   \s
                    {"test.employee.married":false,"test.employee.empname":"Anagha","test.dept.deptno":5,"test.employee.empno":18,
                    "test.employee.joining_date":"2021-10-14","test.employee.dob":"2000-10-14","test.employee.salary":90000.0,
                    "test.employee.deptno":5,"test.dept.deptname":"Accounting and Finance"},
                   \s
                    {"test.employee.married":true,"test.employee.empname":"Diana","test.dept.deptno":5,"test.employee.empno":19,
                    "test.employee.joining_date":"1970-06-03","test.employee.dob":"1945-01-03","test.employee.salary":19700.0,
                    "test.employee.deptno":5,"test.dept.deptname":"Accounting and Finance"},
                   \s
                    {"test.employee.married":false,"test.employee.empname":"Matt","test.dept.deptno":5,"test.employee.empno":20,
                    "test.employee.joining_date":"1989-06-16","test.employee.dob":"1956-12-13","test.employee.salary":33000.0,
                    "test.employee.deptno":5,"test.dept.deptname":"Accounting and Finance"},
                   \s
                    {"test.employee.married":true,"test.employee.empname":"Holt","test.dept.deptno":6,"test.employee.empno":21,
                    "test.employee.joining_date":"1990-01-01","test.employee.dob":"1958-02-25","test.employee.salary":87000.0,
                    "test.employee.deptno":6,"test.dept.deptname":"IT"},
                   \s
                    {"test.employee.married":false,"test.employee.empname":"Peralta","test.dept.deptno":6,"test.employee.empno":22,
                    "test.employee.joining_date":"1990-01-01","test.employee.dob":"1980-01-31","test.employee.salary":22000.0,
                    "test.employee.deptno":6,"test.dept.deptname":"IT"},
                   \s
                    {"test.employee.married":true,"test.employee.empname":"Mando","test.dept.deptno":6,"test.employee.empno":23,
                    "test.employee.joining_date":"1990-01-01","test.employee.dob":"1967-11-30","test.employee.salary":35700.0,
                    "test.employee.deptno":6,"test.dept.deptname":"IT"},
                   \s
                    {"test.employee.married":false,"test.employee.empname":"Vader","test.dept.deptno":6,"test.employee.empno":24,
                    "test.employee.joining_date":"1990-01-01","test.employee.dob":"1959-12-13","test.employee.salary":3000.0,
                    "test.employee.deptno":6,"test.dept.deptname":"IT"}
                ],
                "size":24}
                """ );

        cqlInterfaceTestHelper( "relation test.dept and test.employee", expectedJsonNode, true );
    }


    @Test
    public void testRestCqlRelationOnlyQueryWithOROperator() {
        HttpResponse<JsonNode> response = executeCQL( "relation test.dept or test.employee" );
        int actualSize = response.getBody().getArray().getJSONObject( 0 ).getJSONArray( "data" ).length();

        assertEquals( 144, actualSize );
    }


    @Test
    public void testRestCqlFilterAndRelationQuery() {
        JsonNode expectedJsonNode = new JsonNode( "{\"result\":[{\"test.employee.married\":false,"
                + "\"test.employee.empname\":\"Rhody\",\"test.dept.deptno\":2,\"test.employee.empno\":6,"
                + "\"test.employee.joining_date\":\"1996-11-12\",\"test.employee.dob\":\"1970-07-21\",\"test.employee.salary\":67500.0,"
                + "\"test.employee.deptno\":2,\"test.dept.deptname\":\"Marketing\"},{\"test.employee.married\":false,"
                + "\"test.employee.empname\":\"Mary\",\"test.dept.deptno\":3,\"test.employee.empno\":12,"
                + "\"test.employee.joining_date\":\"1970-04-23\",\"test.employee.dob\":\"1950-12-31\",\"test.employee.salary\":60000.0,"
                + "\"test.employee.deptno\":3,\"test.dept.deptname\":\"Production\"},{\"test.employee.married\":false,"
                + "\"test.employee.empname\":\"Debby\",\"test.dept.deptno\":4,\"test.employee.empno\":14,"
                + "\"test.employee.joining_date\":\"1999-01-01\",\"test.employee.dob\":\"1969-04-30\",\"test.employee.salary\":50000.0,"
                + "\"test.employee.deptno\":4,\"test.dept.deptname\":\"Research and Development\"},"
                + "{\"test.employee.married\":false,\"test.employee.empname\":\"Roy\",\"test.dept.deptno\":4,"
                + "\"test.employee.empno\":16,\"test.employee.joining_date\":\"1991-03-12\",\"test.employee.dob\":\"1966-02-28\","
                + "\"test.employee.salary\":57000.0,\"test.employee.deptno\":4,"
                + "\"test.dept.deptname\":\"Research and Development\"},{\"test.employee.married\":false,"
                + "\"test.employee.empname\":\"Anagha\",\"test.dept.deptno\":5,\"test.employee.empno\":18,"
                + "\"test.employee.joining_date\":\"2021-10-14\",\"test.employee.dob\":\"2000-10-14\",\"test.employee.salary\":90000.0,"
                + "\"test.employee.deptno\":5,\"test.dept.deptname\":\"Accounting and Finance\"}],\"size\":5}" );

        // todo sort by is not implemented correctly for joins, so we can't use it here, disabled value comp for this test for now
        HttpResponse<JsonNode> response = executeCQL(
                "test.employee.salary >= 50000 and test.employee.married == FALSE relation test.dept and test.employee" );
        int actualSize = response.getBody().getArray().getJSONObject( 0 ).getJSONArray( "data" ).length();
        assertEquals( 5, actualSize );
    }


    @Test
    public void testRestCqlProjectionQuery() {
        JsonNode expectedJsonNode = new JsonNode( "{\"result\":[{\"test.employee.empname\":\"Joe\"},"
                + "{\"test.employee.empname\":\"Amy\"},{\"test.employee.empname\":\"Charlie\"},"
                + "{\"test.employee.empname\":\"Ravi\"},{\"test.employee.empname\":\"Imane\"},"
                + "{\"test.employee.empname\":\"Rhody\"},{\"test.employee.empname\":\"Cryer\"},"
                + "{\"test.employee.empname\":\"Lily\"},{\"test.employee.empname\":\"Rose\"},"
                + "{\"test.employee.empname\":\"Happy\"},{\"test.employee.empname\":\"Marcus\"},"
                + "{\"test.employee.empname\":\"Mary\"},{\"test.employee.empname\":\"Joy\"},"
                + "{\"test.employee.empname\":\"Debby\"},{\"test.employee.empname\":\"Troy\"},"
                + "{\"test.employee.empname\":\"Roy\"},{\"test.employee.empname\":\"Rich\"},"
                + "{\"test.employee.empname\":\"Anagha\"},{\"test.employee.empname\":\"Diana\"},"
                + "{\"test.employee.empname\":\"Matt\"},{\"test.employee.empname\":\"Holt\"},"
                + "{\"test.employee.empname\":\"Peralta\"},{\"test.employee.empname\":\"Mando\"},"
                + "{\"test.employee.empname\":\"Vader\"}],\"size\":24}" );

        cqlInterfaceTestHelper( "relation test.dept and test.employee project test.employee.empname", expectedJsonNode, true );
    }


    @Test
    public void testRestCqlProjectionQueryWithAggregation() {
        JsonNode expectedJsonNode = new JsonNode( "{\"result\":[{\"COUNT( test.employee.empno )\":24}],\"size\":1}" );
        cqlInterfaceTestHelper( "relation test.employee project test.employee.empno/count", expectedJsonNode, true );
    }


    @Test
    public void testRestCqlProjectionQueryWithAggregationAndGrouping() {
        JsonNode expectedJsonNode = new JsonNode( "{\"result\":[{\"test.employee.married\":true,"
                + "\"COUNT( test.employee.empno )\":2,\"test.dept.deptname\":\"Human Resources\"},"
                + "{\"test.employee.married\":false,\"COUNT( test.employee.empno )\":2,"
                + "\"test.dept.deptname\":\"Human Resources\"},{\"test.employee.married\":true,"
                + "\"COUNT( test.employee.empno )\":2,\"test.dept.deptname\":\"Marketing\"},"
                + "{\"test.employee.married\":false,\"COUNT( test.employee.empno )\":2,"
                + "\"test.dept.deptname\":\"Marketing\"},{\"test.employee.married\":true,"
                + "\"COUNT( test.employee.empno )\":2,\"test.dept.deptname\":\"Production\"},"
                + "{\"test.employee.married\":false,\"COUNT( test.employee.empno )\":2,"
                + "\"test.dept.deptname\":\"Production\"},{\"test.employee.married\":true,"
                + "\"COUNT( test.employee.empno )\":2,\"test.dept.deptname\":\"Research and Development\"},"
                + "{\"test.employee.married\":false,\"COUNT( test.employee.empno )\":2,"
                + "\"test.dept.deptname\":\"Research and Development\"},{\"test.employee.married\":true,"
                + "\"COUNT( test.employee.empno )\":2,\"test.dept.deptname\":\"Accounting and Finance\"},"
                + "{\"test.employee.married\":false,\"COUNT( test.employee.empno )\":2,"
                + "\"test.dept.deptname\":\"Accounting and Finance\"},{\"test.employee.married\":true,"
                + "\"COUNT( test.employee.empno )\":2,\"test.dept.deptname\":\"IT\"},"
                + "{\"test.employee.married\":false,\"COUNT( test.employee.empno )\":2,"
                + "\"test.dept.deptname\":\"IT\"}],\"size\":12}" );

        // while this test seems intricate, it uses an assertion, which is not safe to assume,
        // due to the fact that different stores can provide different orders of result rows after grouping
        /*cqlInterfaceTestHelper(
                "relation test.employee and test.dept project test.employee.empno/count test.employee.married test.dept.deptname ",
                expectedJsonNode );*/

        // due to the reasoning above, we check only the amount of results for now - DL
        HttpResponse<JsonNode> response = executeCQL( "relation test.employee and test.dept project test.employee.empno/count test.employee.married test.dept.deptname" );
        int actualSize = response.getBody().getArray().getJSONObject( 0 ).getJSONArray( "data" ).length();
        assertEquals( 12, actualSize );
    }


    @Test
    public void testRestCqlSortingQuery() {
        JsonNode expectedJsonNode = new JsonNode( "{\"result\":[{\"test.employee.empname\":\"Amy\"},"
                + "{\"test.employee.empname\":\"Anagha\"},{\"test.employee.empname\":\"Charlie\"},"
                + "{\"test.employee.empname\":\"Cryer\"},{\"test.employee.empname\":\"Debby\"},"
                + "{\"test.employee.empname\":\"Diana\"},{\"test.employee.empname\":\"Happy\"},"
                + "{\"test.employee.empname\":\"Holt\"},{\"test.employee.empname\":\"Imane\"},"
                + "{\"test.employee.empname\":\"Joe\"},{\"test.employee.empname\":\"Joy\"},"
                + "{\"test.employee.empname\":\"Lily\"},{\"test.employee.empname\":\"Mando\"},"
                + "{\"test.employee.empname\":\"Marcus\"},{\"test.employee.empname\":\"Mary\"},"
                + "{\"test.employee.empname\":\"Matt\"},{\"test.employee.empname\":\"Peralta\"},"
                + "{\"test.employee.empname\":\"Ravi\"},{\"test.employee.empname\":\"Rhody\"},"
                + "{\"test.employee.empname\":\"Rich\"},{\"test.employee.empname\":\"Rose\"},"
                + "{\"test.employee.empname\":\"Roy\"},{\"test.employee.empname\":\"Troy\"},"
                + "{\"test.employee.empname\":\"Vader\"}],\"size\":24}" );

        cqlInterfaceTestHelper(
                "relation test.employee sortby test.employee.empname project test.employee.empname",
                expectedJsonNode, true );
    }


    @Test
    public void testCqlFiltersWithJoinsAndProjection() {
        JsonNode expectedJsonNode = new JsonNode( "{\"result\":[{\"test.employee.empname\":\"Imane\"}," +
                "{\"test.employee.empname\":\"Rhody\"},{\"test.employee.empname\":\"Cryer\"}," +
                "{\"test.employee.empname\":\"Lily\"},{\"test.employee.empname\":\"Holt\"}," +
                "{\"test.employee.empname\":\"Peralta\"},{\"test.employee.empname\":\"Mando\"}," +
                "{\"test.employee.empname\":\"Vader\"}],\"size\":8}" );

        cqlInterfaceTestHelper(
                "test.dept.deptname == \"IT\" or test.dept.deptname == \"Marketing\" " +
                        "relation test.dept and test.employee project test.employee.empname",
                expectedJsonNode,
                true );
    }


    private void cqlInterfaceTestHelper( String cqlQuery, JsonNode expectedJsonNode, boolean ignoreOrder ) {
        HttpResponse<JsonNode> response = executeCQL( cqlQuery );
        JSONObject object;
        if ( response.getBody().isArray() ) {
            object = response.getBody().getArray().getJSONObject( 0 );
        } else {
            object = response.getBody().getObject();
        }
        assertJsonObjects( expectedJsonNode.getObject(), object, ignoreOrder );
    }


    /**
     * This is a utility method, which matches and tests the old expected format which repeats
     * the keys for every entry with the atm format of the http-interface, which uses a header <-> data format
     *
     * @param expected results in a result : {} format, where every key is repeated with the data
     * @param actual results in a {header: [key,...], data: [data,...] } format
     * @param ignoreOrder if the results should be checked for order
     */
    private static void assertJsonObjects( JSONObject expected, JSONObject actual, boolean ignoreOrder ) {
        List<String> keys = new ArrayList<>();

        if ( actual.has( "error" ) && actual.getString( "error" ) != null ) {
            throw new RuntimeException( actual.getString( "error" ) );
        }

        // save order of headers keys
        actual.getJSONArray( "header" ).iterator().forEachRemaining( el -> keys.add( ((JSONObject) el).getString( "name" ) ) );
        JSONArray expectedResults = expected.getJSONArray( "result" );
        JSONArray actualResults = actual.getJSONArray( "data" );
        // same amount of results
        assertEquals( expectedResults.length(), actualResults.length() );

        List<JSONArray> actuals = new ArrayList<>( actualResults.toList() );

        for ( Object result : expectedResults ) {
            JSONObject object = (JSONObject) result;
            assertEquals( actual.getJSONArray( "header" ).length(), object.length() );

            JSONArray found = null;

            for ( JSONArray current : actuals ) {
                if ( compareRow( actual, (JSONObject) result, object, keys, false, current ) ) {
                    found = current;
                    break;
                }
            }
            if ( found != null ) {
                actuals.remove( found );
            } else {
                fail( "Expected result not found in actual results" );
            }
        }

    }


    private static boolean compareRow( JSONObject actual, JSONObject result, JSONObject object, List<String> keys, boolean found, JSONArray current ) {
        for ( String key : object.keySet() ) {
            int keyIndex = keys.indexOf( key );
            found = compareEntry( actual, result, key, keyIndex, current );
            if ( !found ) {
                // break current row if not found
                return false;
            }
        }
        return true;
    }


    private static boolean compareEntry( JSONObject actual, JSONObject result, String key, int keyIndex, JSONArray current ) {
        if ( actual.getJSONArray( "header" ).getJSONObject( keyIndex ).getString( "dataType" ).equals( "DATE" ) ) {
            return Objects.equals( LocalDate.ofEpochDay( result.getLong( key ) ), Date.valueOf( current.getString( keyIndex ) ).toLocalDate() );
        }
        return Objects.equals( result.get( key ).toString(), current.get( keyIndex ) );

    }


    private HttpResponse<JsonNode> executeCQL( String cqlQuery ) {
        HttpRequestWithBody request = Unirest.post( "{protocol}://{host}:{port}/cql" );
        request.basicAuth( "pa", "" );
        request.routeParam( "protocol", "http" );
        request.routeParam( "host", "127.0.0.1" );
        request.routeParam( "port", "8087" );
        if ( log.isDebugEnabled() ) {
            log.debug( request.getUrl() );
        }

        JsonObject data = new JsonObject();
        data.addProperty( "query", cqlQuery );
        data.addProperty( "noLimit", true );

        return request.body( data ).header( "Content-ExpressionType", "application/json" ).asJson();
    }

}
