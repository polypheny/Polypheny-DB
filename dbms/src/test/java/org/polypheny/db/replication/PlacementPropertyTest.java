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

package org.polypheny.db.replication;


import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.calcite.avatica.AvaticaSqlException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.polypheny.db.AdapterTestSuite;
import org.polypheny.db.TestHelper.JdbcConnection;


@Category({ AdapterTestSuite.class })
public class PlacementPropertyTest {

    @Test
    public void parseSqlAlterPlacementProperty() throws SQLException {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            try ( Statement statement = connection.createStatement() ) {
                statement.executeUpdate( "CREATE TABLE parsesqlalterplacementproperty( "
                        + "tprimary INTEGER NOT NULL, "
                        + "tinteger INTEGER NULL, "
                        + "tvarchar VARCHAR(20) NULL, "
                        + "PRIMARY KEY (tprimary) )" );

                try {
                    // Verify general specification for property: 'ROLE'
                    statement.executeUpdate( "ALTER TABLE parsesqlalterplacementproperty "
                            + "MODIFY PLACEMENT "
                            + "ON STORE hsqldb SET ROLE UPTODATE" );

                    // Verify another specification for property: 'ROLE'
                    statement.executeUpdate( "ALTER TABLE parsesqlalterplacementproperty "
                            + "MODIFY PLACEMENT "
                            + "ON STORE hsqldb "
                            + "SET ROLE REFRESHABLE" );

                    // Check assert False. Wrong placement property value for property 'ROLE'
                    boolean failed = false;
                    try {
                        statement.executeUpdate( "ALTER TABLE parsesqlalterplacementproperty "
                                + "MODIFY PLACEMENT "
                                + "ON STORE hsqldb "
                                + "SET ROLE randomValue" );
                    } catch ( AvaticaSqlException e ) {
                        failed = true;
                    }
                    Assert.assertTrue( failed );

                    // Check assert False. Wrong placement property
                    failed = false;
                    try {
                        statement.executeUpdate( "ALTER TABLE parsesqlalterplacementproperty "
                                + "MODIFY PLACEMENT "
                                + "ON STORE hsqldb "
                                + "SET randomProperty UPTODATE" );
                    } catch ( AvaticaSqlException e ) {
                        failed = true;
                    }
                    Assert.assertTrue( failed );

                    // Check assert False. Incomplete specification
                    failed = false;
                    try {
                        statement.executeUpdate( "ALTER TABLE parsesqlalterplacementproperty "
                                + "MODIFY PLACEMENT "
                                + "ON STORE hsqldb"
                                + "SET" );
                    } catch ( AvaticaSqlException e ) {
                        failed = true;
                    }
                    Assert.assertTrue( failed );


                } finally {
                    // Drop tables and stores
                    statement.executeUpdate( "DROP TABLE parsesqlalterplacementproperty" );
                }
            }
        }
    }

}
