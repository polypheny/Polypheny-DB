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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import java.sql.*;

public class OracleConnection {

    public static void main(String[] args) {
        // Verbindungseinstellungen
        String url = "jdbc:oracle:thin:@localhost:1521/XE";
        String username = "system";
        String password = "roman123";

        // SQL-Abfrage
        String query = "SELECT * FROM test";

        try (Connection conn = DriverManager.getConnection(url, username, password);
                Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery(query)) {

            // Metadaten holen (z. B. Spaltenanzahl und -namen)
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            // Alle Zeilen durchlaufen
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(rs.getString(i));
                    if (i < columnCount) System.out.print(" | ");
                }
                System.out.println();
            }

        } catch (SQLException e) {
            System.out.println("Fehler bei der Verbindung oder Abfrage:");
            e.printStackTrace();
        }
    }
}

/*
// Überprüfen, ob das ResultSet Daten enthält
            if (rs.next()) {
                // Angenommen, die Tabelle hat eine Spalte "spalte" (beispielhaft)
                int spalte = rs.getInt("spalte");
                System.out.println("Wert aus Spalte: " + spalte);
            } else {
                System.out.println("Keine Daten gefunden.");
            }
 */
