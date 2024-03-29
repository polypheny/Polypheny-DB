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

package org.polypheny.db.interpreter;


import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.polypheny.db.algebra.AlgFieldCollation;
import org.polypheny.db.algebra.core.Sort;
import org.polypheny.db.rex.RexLiteral;


/**
 * Interpreter node that implements a {@link Sort}.
 */
public class SortNode extends AbstractSingleNode<Sort> {

    public SortNode( Compiler compiler, Sort alg ) {
        super( compiler, alg );
    }


    @Override
    public void run() throws InterruptedException {
        final int offset =
                alg.offset == null
                        ? 0
                        : ((RexLiteral) alg.offset).value.asInteger().value;
        final int fetch =
                alg.fetch == null
                        ? -1
                        : ((RexLiteral) alg.fetch).value.asInteger().value;
        // In pure limit mode. No sort required.
        Row row;
        loop:
        if ( alg.getCollation().getFieldCollations().isEmpty() ) {
            for ( int i = 0; i < offset; i++ ) {
                row = source.receive();
                if ( row == null ) {
                    break loop;
                }
            }
            if ( fetch >= 0 ) {
                for ( int i = 0; i < fetch && (row = source.receive()) != null; i++ ) {
                    sink.send( row );
                }
            } else {
                while ( (row = source.receive()) != null ) {
                    sink.send( row );
                }
            }
        } else {
            // Build a sorted collection.
            final List<Row> list = new ArrayList<>();
            while ( (row = source.receive()) != null ) {
                list.add( row );
            }
            list.sort( comparator() );
            final int end = fetch < 0 || offset + fetch > list.size()
                    ? list.size()
                    : offset + fetch;
            for ( int i = offset; i < end; i++ ) {
                sink.send( list.get( i ) );
            }
        }
        sink.end();
    }


    private Comparator<Row> comparator() {
        if ( alg.getCollation().getFieldCollations().size() == 1 ) {
            return comparator( alg.getCollation().getFieldCollations().get( 0 ) );
        }
        return Ordering.compound( Iterables.transform( alg.getCollation().getFieldCollations(), this::comparator ) );
    }


    private Comparator<Row> comparator( AlgFieldCollation fieldCollation ) {
        final int nullComparison = fieldCollation.nullDirection.nullComparison;
        final int x = fieldCollation.getFieldIndex();
        switch ( fieldCollation.direction ) {
            case ASCENDING:
                return ( o1, o2 ) -> {
                    final Comparable c1 = (Comparable) o1.getValues()[x];
                    final Comparable c2 = (Comparable) o2.getValues()[x];
                    return AlgFieldCollation.compare( c1, c2, nullComparison );
                };
            default:
                return ( o1, o2 ) -> {
                    final Comparable c1 = (Comparable) o1.getValues()[x];
                    final Comparable c2 = (Comparable) o2.getValues()[x];
                    return AlgFieldCollation.compare( c2, c1, -nullComparison );
                };
        }
    }

}

