/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.webui.schemaDiscovery;

import org.polypheny.db.webui.schemaDiscovery.DataHandling.AttributeInfo;
import org.polypheny.db.webui.schemaDiscovery.DataHandling.DatabaseInfo;
import org.polypheny.db.webui.schemaDiscovery.DataHandling.SchemaInfo;
import org.polypheny.db.webui.schemaDiscovery.DataHandling.TableInfo;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class PostgreSqlConnection {

    static String host = "localhost";
    static String port = "5432";
    static String user = "postgres";
    static String password = "password";


    public static List<DatabaseInfo> getDatabasesSchemasAndTables() throws SQLException {
        List<DatabaseInfo> dbs = new ArrayList<>();

        String metaUrl = "jdbc:postgresql://" + host + ":" + port + "/postgres";
        try (
                Connection metaConn = DriverManager.getConnection(metaUrl, user, password);
                Statement stmt = metaConn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT datname FROM pg_database WHERE datistemplate = false")
        ) {
            while (rs.next()) {
                String dbName = rs.getString("datname");
                DatabaseInfo dbInfo = new DatabaseInfo(dbName);

                String dbUrl = "jdbc:postgresql://" + host + ":" + port + "/" + dbName;
                try (Connection dbConn = DriverManager.getConnection(dbUrl, user, password)) {
                    DatabaseMetaData meta = dbConn.getMetaData();

                    ResultSet schemas = meta.getSchemas();
                    while (schemas.next()) {
                        String schemaName = schemas.getString("TABLE_SCHEM");
                        SchemaInfo schema = new SchemaInfo(schemaName);

                        ResultSet tables = meta.getTables(null, schemaName, "%", new String[]{"TABLE"});
                        while (tables.next()) {
                            String tableName = tables.getString("TABLE_NAME");
                            TableInfo table = new TableInfo(tableName);

                            ResultSet columns = meta.getColumns(null, schemaName, tableName, "%");
                            while (columns.next()) {
                                String columnName = columns.getString("COLUMN_NAME");
                                String columnType = columns.getString("TYPE_NAME");

                                AttributeInfo attribute = new AttributeInfo(columnName, columnType);

                                String sampleQuery = "SELECT \"" + columnName + "\" FROM \"" + schemaName + "\".\"" + tableName + "\" LIMIT 20";
                                try (
                                        Statement sampleStmt = dbConn.createStatement();
                                        ResultSet sampleRs = sampleStmt.executeQuery(sampleQuery)
                                ) {
                                    while (sampleRs.next()) {
                                        Object value = sampleRs.getObject(columnName);
                                        attribute.sampleValues.add(value != null ? value.toString() : "NULL");
                                    }
                                } catch (SQLException e) {
                                    System.err.println("Fehler beim Abrufen von Beispieldaten für Spalte " + columnName + ": " + e.getMessage());
                                }

                                table.attributes.add(attribute);
                            }

                            schema.tables.add(table);
                        }

                        dbInfo.schemas.add(schema);
                    }

                } catch (SQLException e) {
                    System.err.println("Fehler beim Abrufen von Schemas für DB " + dbName + ": " + e.getMessage());
                }

                dbs.add(dbInfo);
            }
        }

        return dbs;
    }





    public static void main(String[] args) {
        try {
            List<DatabaseInfo> dbs = getDatabasesSchemasAndTables();
            for (DatabaseInfo db : dbs) {
                System.out.print(db.toString());
            }
            JsonExport.printAsJson( dbs );

        } catch (SQLException e) {
            System.err.println("Fehler bei der Schema-Erkennung: " + e.getMessage());
            e.printStackTrace();
        }
    }

}
