/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.webui;


import ch.unibas.dmi.dbis.polyphenydb.PolySqlType;
import ch.unibas.dmi.dbis.polyphenydb.catalog.Catalog.SchemaType;
import ch.unibas.dmi.dbis.polyphenydb.catalog.Catalog.TableType;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogColumn;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogDatabase;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogDefaultValue;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogKey;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogSchema;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogTable;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.CatalogUser;
import ch.unibas.dmi.dbis.polyphenydb.catalog.entity.combined.CatalogCombinedTable;
import java.util.ArrayList;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Test;


public class SchemaToJsonMapperTest {


    private static final String mockJson = "{\"tableName\":\"stores\",\"columns\":[{\"columnName\":\"sid\",\"type\":\"INTEGER\",\"nullable\":false},{\"columnName\":\"name\",\"type\":\"VARCHAR\",\"length\":50,\"nullable\":false},{\"columnName\":\"location\",\"type\":\"VARCHAR\",\"length\":30,\"nullable\":true,\"defaultValue\":\"Basel\"}],\"primaryKeyColumnNames\":[\"sid\",\"name\"]}";


    @Test
    public void exportTest() {
        CatalogCombinedTable catalogCombinedTable = new CatalogCombinedTable(
                new CatalogTable( 4, "stores", 1, "public", 1, "APP", 1, "hans", TableType.TABLE, "", 23L ),
                Arrays.asList(
                        new CatalogColumn( 5, "sid", 4, "stores", 1, "public", 1, "APP", 1, PolySqlType.INTEGER, null, null, false, null, null ),
                        new CatalogColumn( 6, "name", 4, "stores", 1, "public", 1, "APP", 2, PolySqlType.VARCHAR, 50, null, false, null, null ),
                        new CatalogColumn( 7, "location", 4, "stores", 1, "public", 1, "APP", 3, PolySqlType.VARCHAR, 30, null, true, null, new CatalogDefaultValue( 7, PolySqlType.VARCHAR, "Basel", null ) )
                ),
                new CatalogSchema( 1, "public", 1, "APP", 1, "hans", SchemaType.RELATIONAL ),
                new CatalogDatabase( 1, "APP", 1, "hans", 1L, "public" ),
                new CatalogUser( 1, "hans", "secrete" ),
                new ArrayList<>(),
                Arrays.asList(
                        new CatalogKey( 23L, 4, "stores", 1, "public", 1, "APP", Arrays.asList( 5L, 6L ), Arrays.asList( "sid", "name" ) ),
                        new CatalogKey( 24L, 4, "stores", 1, "public", 1, "APP", Arrays.asList( 6L ), Arrays.asList( "name" ) )
                )
        );
        String json = SchemaToJsonMapper.exportTableDefinitionAsJson( catalogCombinedTable, true, true );
        Assert.assertEquals( json, mockJson );
    }


    @Test
    public void getStatementTest() {
        String statement = SchemaToJsonMapper.getCreateTableStatementFromJson( mockJson, true, true, "foo", null, "hsqldb1" );
        final String expected1 = "CREATE TABLE \"foo\".\"stores\" (\"sid\" INTEGER NOT NULL, \"name\" VARCHAR(50) NOT NULL, \"location\" VARCHAR(30) DEFAULT 'Basel', PRIMARY KEY(\"sid\", \"name\")) ON STORE \"hsqldb1\"";
        Assert.assertEquals( statement, expected1 );

        statement = SchemaToJsonMapper.getCreateTableStatementFromJson( mockJson, false, false, "foo", null, null );
        final String expected2 = "CREATE TABLE \"foo\".\"stores\" (\"sid\" INTEGER NOT NULL, \"name\" VARCHAR(50) NOT NULL, \"location\" VARCHAR(30))";
        Assert.assertEquals( statement, expected2 );

        statement = SchemaToJsonMapper.getCreateTableStatementFromJson( mockJson, false, false, "foo", "bar", null );
        final String expected3 = "CREATE TABLE \"foo\".\"bar\" (\"sid\" INTEGER NOT NULL, \"name\" VARCHAR(50) NOT NULL, \"location\" VARCHAR(30))";
        Assert.assertEquals( statement, expected3 );
    }


    @Test
    public void getTableNameTest() {
        String name = SchemaToJsonMapper.getTableNameFromJson( mockJson );
        final String expected = "stores";
        Assert.assertEquals( name, expected );
    }

}
