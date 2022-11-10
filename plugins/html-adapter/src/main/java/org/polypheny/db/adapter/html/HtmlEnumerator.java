/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.adapter.html;


import java.util.Iterator;
import org.apache.calcite.linq4j.Enumerator;
import org.jsoup.select.Elements;


/**
 * Wraps {@link HtmlReader} and {@link HtmlRowConverter}, enumerates tr DOM elements as table rows.
 */
class HtmlEnumerator implements Enumerator<Object> {

    private final Iterator<Elements> iterator;
    private final HtmlRowConverter converter;
    private final int[] fields;
    private Object current;


    HtmlEnumerator( Iterator<Elements> iterator, HtmlRowConverter converter ) {
        this( iterator, converter, identityList( converter.width() ) );
    }


    HtmlEnumerator( Iterator<Elements> iterator, HtmlRowConverter converter, int[] fields ) {
        this.iterator = iterator;
        this.converter = converter;
        this.fields = fields;
    }


    @Override
    public Object current() {
        if ( current == null ) {
            this.moveNext();
        }
        return current;
    }


    @Override
    public boolean moveNext() {
        try {
            if ( this.iterator.hasNext() ) {
                final Elements row = this.iterator.next();
                current = this.converter.toRow( row, this.fields );
                return true;
            } else {
                current = null;
                return false;
            }
        } catch ( RuntimeException | Error e ) {
            throw e;
        } catch ( Exception e ) {
            throw new RuntimeException( e );
        }
    }


    // required by linq4j Enumerator interface
    @Override
    public void reset() {
        throw new UnsupportedOperationException();
    }


    // required by linq4j Enumerator interface
    @Override
    public void close() {
    }


    /**
     * Returns an array of integers {0, ..., n - 1}.
     */
    private static int[] identityList( int n ) {
        int[] integers = new int[n];

        for ( int i = 0; i < n; i++ ) {
            integers[i] = i;
        }

        return integers;
    }

}

