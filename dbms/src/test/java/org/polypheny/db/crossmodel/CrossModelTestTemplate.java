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

package org.polypheny.db.crossmodel;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.function.Consumer;
import org.polypheny.db.TestHelper.JdbcConnection;

public class CrossModelTestTemplate {

    @SafeVarargs
    public static void executeStatements( SqlConsumer<Statement>... statementConsumers ) {
        executeStatements( List.of( statementConsumers ) );
    }


    public static void executeStatements( List<SqlConsumer<Statement>> statementConsumers ) {
        try ( JdbcConnection polyphenyDbConnection = new JdbcConnection( true ) ) {
            Connection connection = polyphenyDbConnection.getConnection();
            for ( SqlConsumer<Statement> consumer : statementConsumers ) {
                executeStatement( connection, consumer );
            }

        } catch ( Exception e ) {
            throw new RuntimeException( e );
        }
    }


    public static void executeStatement( Connection connection, SqlConsumer<Statement> statementConsumer ) throws SQLException {
        try ( Statement statement = connection.createStatement() ) {
            statementConsumer.accept( statement );
        }
    }


    public interface SqlConsumer<T> extends Consumer<T> {

        @Override
        default void accept( T t ) {
            try {
                tryAccept( t );
            } catch ( Exception e ) {
                throw new RuntimeException( e );
            }
        }

        void tryAccept( T t ) throws Exception;

    }

}
