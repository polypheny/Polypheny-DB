/*
 * Copyright 2019-2021 The Polypheny Project
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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.test;


import java.io.PrintWriter;
import java.io.Writer;
import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.piglet.Handler;
import org.polypheny.db.tools.PigAlgBuilder;


/**
 * Extension to {@link Handler} that can execute commands using Polypheny-DB.
 */
class PolyphenyDbHandler extends Handler {

    private final PrintWriter writer;


    PolyphenyDbHandler( PigAlgBuilder builder, Writer writer ) {
        super( builder );
        this.writer = new PrintWriter( writer );
    }


    @Override
    protected void dump( AlgNode alg ) {
        throw new RuntimeException( "Unsupported!" );
        /* try ( PreparedStatement preparedStatement = RelRunners.run( alg ) ) {
            final ResultSet resultSet = preparedStatement.executeQuery();
            dump( resultSet, true );
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        } */
    }


    private void dump( ResultSet resultSet, boolean newline ) throws SQLException {
        final int columnCount = resultSet.getMetaData().getColumnCount();
        int r = 0;
        while ( resultSet.next() ) {
            if ( !newline && r++ > 0 ) {
                writer.print( "," );
            }
            if ( columnCount == 0 ) {
                if ( newline ) {
                    writer.println( "()" );
                } else {
                    writer.print( "()" );
                }
            } else {
                writer.print( '(' );
                dumpColumn( resultSet, 1 );
                for ( int i = 2; i <= columnCount; i++ ) {
                    writer.print( ',' );
                    dumpColumn( resultSet, i );
                }
                if ( newline ) {
                    writer.println( ')' );
                } else {
                    writer.print( ")" );
                }
            }
        }
    }


    /**
     * Dumps a column value.
     *
     * @param i Column ordinal, 1-based
     */
    private void dumpColumn( ResultSet resultSet, int i ) throws SQLException {
        final int t = resultSet.getMetaData().getColumnType( i );
        switch ( t ) {
            case Types.ARRAY:
                final Array array = resultSet.getArray( i );
                writer.print( "{" );
                dump( array.getResultSet(), false );
                writer.print( "}" );
                return;
            case Types.REAL:
                writer.print( resultSet.getString( i ) );
                writer.print( "F" );
                return;
            default:
                writer.print( resultSet.getString( i ) );
        }
    }

}

