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

package ch.unibas.dmi.dbis.polyphenydb.adapter.elasticsearch;


import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.collect.AbstractSequentialIterator;
import com.google.common.collect.Iterators;

import java.util.Collections;
import java.util.Iterator;
import java.util.Objects;
import java.util.function.Consumer;


/**
 * "Iterator" which retrieves results lazily and in batches. Uses <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/search-request-scroll.html">Elastic Scrolling API</a>
 * to optimally consume large search results.
 *
 * This class is <strong>not thread safe</strong>.
 */
class Scrolling {

    private final ElasticsearchTransport transport;
    private final int fetchSize;


    Scrolling( ElasticsearchTransport transport ) {
        this.transport = Objects.requireNonNull( transport, "transport" );
        final int fetchSize = transport.fetchSize;
        Preconditions.checkArgument( fetchSize > 0, "invalid fetch size. Expected %s > 0", fetchSize );
        this.fetchSize = fetchSize;
    }


    Iterator<ElasticsearchJson.SearchHit> query( ObjectNode query ) {
        Objects.requireNonNull( query, "query" );
        final long limit;
        if ( query.has( "size" ) ) {
            limit = query.get( "size" ).asLong();
            if ( fetchSize > limit ) {
                // don't use scrolling when batch size is greater than limit
                return transport.search().apply( query ).searchHits().hits().iterator();
            }
        } else {
            limit = Long.MAX_VALUE;
        }

        query.put( "size", fetchSize );
        final ElasticsearchJson.Result first = transport.search( Collections.singletonMap( "scroll", "1m" ) ).apply( query );

        AutoClosingIterator iterator = new AutoClosingIterator( new SequentialIterator( first, transport, limit ), scrollId -> transport.closeScroll( Collections.singleton( scrollId ) ) );

        Iterator<ElasticsearchJson.SearchHit> result = flatten( iterator );
        // apply limit
        if ( limit != Long.MAX_VALUE ) {
            result = Iterators.limit( result, (int) limit );
        }

        return result;
    }


    /**
     * Combines lazily multiple {@link ElasticsearchJson.Result} into a single iterator of {@link ElasticsearchJson.SearchHit}.
     */
    private static Iterator<ElasticsearchJson.SearchHit> flatten( Iterator<ElasticsearchJson.Result> results ) {
        final Iterator<Iterator<ElasticsearchJson.SearchHit>> inputs = Iterators.transform( results, input -> input.searchHits().hits().iterator() );
        return Iterators.concat( inputs );
    }


    /**
     * Observes when existing iterator has ended and clears context (scroll) if any.
     */
    private static class AutoClosingIterator implements Iterator<ElasticsearchJson.Result>, AutoCloseable {

        private final Iterator<ElasticsearchJson.Result> delegate;
        private final Consumer<String> closer;

        /**
         * Was {@link #closer} consumer already called ?
         */
        private boolean closed;

        /**
         * Keeps last value of {@code scrollId} in memory so scroll can be released upon termination
         */
        private String scrollId;


        private AutoClosingIterator( final Iterator<ElasticsearchJson.Result> delegate, final Consumer<String> closer ) {
            this.delegate = delegate;
            this.closer = closer;
        }


        @Override
        public void close() {
            if ( !closed && scrollId != null ) {
                // close once (if scrollId is present)
                closer.accept( scrollId );
            }
            closed = true;
        }


        @Override
        public boolean hasNext() {
            final boolean hasNext = delegate.hasNext();
            if ( !hasNext ) {
                close();
            }
            return hasNext;
        }


        @Override
        public ElasticsearchJson.Result next() {
            ElasticsearchJson.Result next = delegate.next();
            next.scrollId().ifPresent( id -> scrollId = id );
            return next;
        }
    }


    /**
     * Iterator which consumes current {@code scrollId} until full search result is fetched or {@code limit} is reached.
     */
    private static class SequentialIterator extends AbstractSequentialIterator<ElasticsearchJson.Result> {

        private final ElasticsearchTransport transport;
        private final long limit;
        private long count;


        private SequentialIterator( final ElasticsearchJson.Result first, final ElasticsearchTransport transport, final long limit ) {
            super( first );
            this.transport = transport;
            Preconditions.checkArgument( limit >= 0, "limit: %s >= 0", limit );
            this.limit = limit;
        }


        @Override
        protected ElasticsearchJson.Result computeNext( final ElasticsearchJson.Result previous ) {
            final int hits = previous.searchHits().hits().size();
            if ( hits == 0 || count >= limit ) {
                // stop (re-)requesting when limit is reached or no more results
                return null;
            }

            count += hits;
            final String scrollId = previous.scrollId().orElseThrow( () -> new IllegalStateException( "scrollId has to be present" ) );

            return transport.scroll().apply( scrollId );
        }
    }
}
