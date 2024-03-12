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
import java.util.List;
import java.util.stream.IntStream;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.ResultIterator;
import org.polypheny.db.algebra.AlgCollation;
import org.polypheny.db.algebra.AlgCollations;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.piglet.Handler;
import org.polypheny.db.tools.PigAlgBuilder;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;


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
        Transaction transaction = PigletParserTest.transaction;
        Statement statement = transaction.createStatement();

        final List<Pair<Integer, String>> fields = Pair.zip( IntStream.range( 0, alg.getTupleType().getFieldCount() ).boxed().toList(), alg.getTupleType().getFieldNames() );
        final AlgCollation collation =
                alg instanceof Sort
                        ? ((Sort) alg).collation
                        : AlgCollations.EMPTY;

        AlgRoot root = new AlgRoot( alg, alg.getTupleType(), Kind.SELECT, fields, collation );

        // Prepare
        PolyImplementation polyImplementation = statement.getQueryProcessor().prepareQuery( root, true );

        ResultIterator iterator = polyImplementation.execute( statement, -1 );
        dump( iterator, true );

        /* try ( PreparedStatement preparedStatement = RelRunners.run( alg ) ) {
            final ResultSet resultSet = preparedStatement.executeQuery();
            dump( resultSet, true );
        } catch ( SQLException e ) {
            throw new RuntimeException( e );
        } */
    }


    private void dump( ResultIterator iterator, boolean newline ) {
        List<List<PolyValue>> result = iterator.getAllRowsAndClose();
        final int columnCount = result.isEmpty() ? 0 : result.get( 0 ).size();
        int r = 0;
        for ( List<PolyValue> values : result ) {
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
                dumpColumn( values, 1 );
                for ( int i = 2; i <= columnCount; i++ ) {
                    writer.print( ',' );
                    dumpColumn( values, i );
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
    private void dumpColumn( List<PolyValue> tuple, int i ) {
        final PolyValue value = tuple.get( i );

        writer.print( value.toJson() );
        /*switch ( value.getType() ) {
            case PolyType.ARRAY:
                final Array array = resultSet.getArray( i );
                writer.print( "{" );
                dump( array.getResultSet(), false );
                writer.print( "}" );
                return;
            case PolyType.FLOAT:
                writer.print( resultSet.getString( i ) );
                writer.print( "F" );
                return;
            default:
                writer.print( resultSet.getString( i ) );
        }*/
    }

}

