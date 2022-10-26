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

package org.polypheny.db.postgres;

import org.junit.BeforeClass;
import org.junit.Test;
import org.polypheny.db.TestHelper;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
//import static org.polypheny.db.postgresql.PGInterfaceInboundCommunicationHandler.ctx;

public class PGInterfaceIntegrationTests {

    //select: SELECT * FROM public.PGInterfaceTestTable
    private String dqlQuerySentByClient = "P\u0000\u0000\u00001\u0000SELECT * FROM public.PGInterfaceTestTable\u0000\u0000\u0000B\u0000\u0000\u0000\f\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000D\u0000\u0000\u0000\u0006P\u0000E\u0000\u0000\u0000\t\u0000\u0000\u0000\u0000\u0000S\u0000\u0000\u0000\u0004";

    //insert: INSERT INTO public.PGInterfaceTestTable(PkIdTest, VarcharTest, IntTest) VALUES (1, 'Franz', 1), (2, 'Hello', 2), (3, 'By', 3);
    private String dmlQuerySentByClient = "P\u0000\u0000\u0000�\u0000INSERT INTO public.PGInterfaceTestTable(PkIdTest, VarcharTest, IntTest) VALUES (1, 'Franz', 1), (2, 'Hello', 2), (3, 'By', 3)\u0000\u0000\u0000B\u0000\u0000\u0000\f\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000D\u0000\u0000\u0000\u0006P\u0000E\u0000\u0000\u0000\t\u0000\u0000\u0000\u0000\u0001S\u0000\u0000\u0000\u0004";

    //create table: CREATE TABLE public.PGInterfaceTestTable(PkIdTest INTEGER NOT NULL, VarcharTest VARCHAR(255), IntTest INTEGER,PRIMARY KEY (PkIdTest))
    private String ddlQuerySentByClient = "P\u0000\u0000\u0000�\u0000CREATE TABLE public.PGInterfaceTestTable(PkIdTest INTEGER NOT NULL, VarcharTest VARCHAR(255), IntTest INTEGER,PRIMARY KEY (PkIdTest))\u0000\u0000\u0000B\u0000\u0000\u0000\f\u0000\u0000\u0000\u0000\u0000\u0000\u0000\u0000D\u0000\u0000\u0000\u0006P\u0000E\u0000\u0000\u0000\t\u0000\u0000\u0000\u0000\u0001S\u0000\u0000\u0000\u0004";
            //new Object[]{"REAL'S HOWTO"};

    private Connection c;


    @BeforeClass
    public static void start() throws SQLException {
        // Ensures that Polypheny-DB is running
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance();
    }



    @Test
    public int testIfDDLIsExecuted() {

        try {
            Statement statement = c.createStatement();

                /*
                System.out.println("executing select");
                ResultSet rs = statement.executeQuery("SELECT * FROM public.emps"); //empid, deptno, name, salary, commission
                //ResultSet rs = statement.executeQuery("SELECT * FROM public.Album"); //AlbumId, Title, ArtistId
                System.out.println("SQL-part executed successfully");

                while (rs.next()) {
                    int empid = rs.getInt("empid");
                    int deptno = rs.getInt("deptno");
                    String name = rs.getString("name");
                    int salary = rs.getInt("salary");
                    int commission = rs.getInt("commission");

                    //System.out.printf( "AlbumId = %s , Title = %s, ArtistId = %s ", albumid,title, artistid );
                    System.out.printf("LolId = %s \n", empid);
                    System.out.printf("deptno = %s \n", deptno);
                    System.out.printf("name = %s \n", name);
                    System.out.printf("salary = %s \n", salary);
                    System.out.printf("commission = %s \n", commission);
                    System.out.println();

                }

                 */

            int r = statement.executeUpdate("INSERT INTO public.Album(AlbumId, Title, ArtistId) VALUES (3, 'Lisa', 3);");

            //rs.close();
            statement.close();
            return r;

        } catch (Exception e) {
            e.printStackTrace();
        }

        return 0;
    }

    /*

    @Test
    public void testIfDDLIsExecuted() {
        PGInterfaceInboundCommunicationHandler mockInterfaceInboundCommunicationHandler = mock(PGInterfaceInboundCommunicationHandler.class);
        mockInterfaceInboundCommunicationHandler.decideCycle( ddlQuerySentByClient );

        //TODO(FF): look if result was executed in Polypheny
    }

    @Test
    public void testIfDMLIsExecuted() {
        PGInterfaceInboundCommunicationHandler mockInterfaceInboundCommunicationHandler = mock(PGInterfaceInboundCommunicationHandler.class);
        mockInterfaceInboundCommunicationHandler.decideCycle( dmlQuerySentByClient );

        //TODO(FF): look if result was executed in Polypheny
    }

    @Test
    public void testIfDQLIsExecuted() {
        PGInterfaceInboundCommunicationHandler mockInterfaceInboundCommunicationHandler = mock(PGInterfaceInboundCommunicationHandler.class);
        mockInterfaceInboundCommunicationHandler.decideCycle( dqlQuerySentByClient );

        //TODO(FF): look if result was executed in Polypheny
    }

     */
}
