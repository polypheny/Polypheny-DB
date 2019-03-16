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
 *  The MIT License (MIT)
 *
 *  Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a
 *  copy of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.test;


import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.lang.ref.SoftReference;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


/**
 * Set of queries against the FoodMart database.
 */
public class FoodMartQuerySet {

    private static SoftReference<FoodMartQuerySet> ref;

    public final Map<Integer, FoodmartQuery> queries = new LinkedHashMap<>();


    private FoodMartQuerySet() throws IOException {
        final ObjectMapper mapper = new ObjectMapper();
        mapper.configure( JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true );
        mapper.configure( JsonParser.Feature.ALLOW_SINGLE_QUOTES, true );

        final InputStream inputStream = new net.hydromatic.foodmart.queries.FoodmartQuerySet().getQueries();
        FoodmartRoot root = mapper.readValue( inputStream, FoodmartRoot.class );
        for ( FoodmartQuery query : root.queries ) {
            queries.put( query.id, query );
        }
    }


    /**
     * Returns the singleton instance of the query set. It is backed by a soft reference, so it may be freed if memory is short and no one is using it.
     */
    public static synchronized FoodMartQuerySet instance() throws IOException {
        final SoftReference<FoodMartQuerySet> refLocal = ref;
        if ( refLocal != null ) {
            final FoodMartQuerySet set = refLocal.get();
            if ( set != null ) {
                return set;
            }
        }
        final FoodMartQuerySet set = new FoodMartQuerySet();
        ref = new SoftReference<>( set );
        return set;
    }


    /**
     * JSON root element.
     */
    public static class FoodmartRoot {

        public final List<FoodmartQuery> queries = new ArrayList<>();
    }


    /**
     * JSON query element.
     */
    public static class FoodmartQuery {

        public int id;
        public String sql;
        public final List<FoodmartColumn> columns = new ArrayList<>();
        public final List<List> rows = new ArrayList<>();
    }


    /**
     * JSON column element.
     */
    public static class FoodmartColumn {

        public String name;
        public String type;
    }
}


