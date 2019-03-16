/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
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
 */

package ch.unibas.dmi.dbis.polyphenydb.test;


import ch.unibas.dmi.dbis.polyphenydb.piglet.Handler;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelRunners;
import ch.unibas.dmi.dbis.polyphenydb.piglet.Handler;
import ch.unibas.dmi.dbis.polyphenydb.rel.RelNode;
import ch.unibas.dmi.dbis.polyphenydb.tools.PigRelBuilder;
import ch.unibas.dmi.dbis.polyphenydb.tools.RelRunners;

import java.io.PrintWriter;
import java.io.Writer;
import java.sql.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;


/**
 * Extension to {@link Handler} that can execute commands using Polypheny-DB.
 */
class PolyphenyDbHandler extends Handler {

    private final PrintWriter writer;


    PolyphenyDbHandler( PigRelBuilder builder, Writer writer ) {
        super( builder );
        this.writer = new PrintWriter( writer );
    }


    @Override
    protected void dump( RelNode rel ) {
        try ( PreparedStatement preparedStatement = RelRunners.run( rel ) ) {
            final ResultSet resultSet = preparedStatement.executeQuery();
            dump( resultSet, true );
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        }
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

