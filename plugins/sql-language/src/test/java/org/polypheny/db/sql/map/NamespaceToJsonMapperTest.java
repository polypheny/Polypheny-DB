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

package org.polypheny.db.sql.map;


import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;
import org.polypheny.db.sql.SqlLanguageDependent;
import org.polypheny.db.sql.web.SchemaToJsonMapper;


public class NamespaceToJsonMapperTest extends SqlLanguageDependent {


    private static final String mockJson = "{\"tableName\":\"stores\",\"columns\":[{\"columnName\":\"sid\",\"type\":\"INTEGER\",\"nullable\":false},{\"columnName\":\"name\",\"type\":\"VARCHAR\",\"length\":50,\"nullable\":false},{\"columnName\":\"location\",\"type\":\"VARCHAR\",\"length\":30,\"nullable\":true,\"defaultValue\":\"Basel\"}],\"primaryKeyColumnNames\":[\"sid\",\"name\"]}";



    @Test
    public void getStatementTest() {
        String statement = SchemaToJsonMapper.getCreateTableStatementFromJson( mockJson, true, true, "foo", null, "hsqldb1" );
        final String expected1 = "CREATE TABLE \"foo\".\"stores\" (\"sid\" INTEGER NOT NULL, \"name\" VARCHAR(50) NOT NULL, \"location\" VARCHAR(30) DEFAULT 'Basel', PRIMARY KEY(\"sid\", \"name\")) ON STORE \"hsqldb1\"";
        assertEquals( statement, expected1 );

        statement = SchemaToJsonMapper.getCreateTableStatementFromJson( mockJson, false, false, "foo", null, null );
        final String expected2 = "CREATE TABLE \"foo\".\"stores\" (\"sid\" INTEGER NOT NULL, \"name\" VARCHAR(50) NOT NULL, \"location\" VARCHAR(30))";
        assertEquals( statement, expected2 );

        statement = SchemaToJsonMapper.getCreateTableStatementFromJson( mockJson, false, false, "foo", "bar", null );
        final String expected3 = "CREATE TABLE \"foo\".\"bar\" (\"sid\" INTEGER NOT NULL, \"name\" VARCHAR(50) NOT NULL, \"location\" VARCHAR(30))";
        assertEquals( statement, expected3 );
    }


    @Test
    public void getTableNameTest() {
        String name = SchemaToJsonMapper.getTableNameFromJson( mockJson );
        final String expected = "stores";
        assertEquals( name, expected );
    }

}
