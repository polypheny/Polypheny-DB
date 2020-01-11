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

package ch.unibas.dmi.dbis.polyphenydb.adapter.geode.rel;


import ch.unibas.dmi.dbis.polyphenydb.adapter.geode.util.GeodeUtils;
import ch.unibas.dmi.dbis.polyphenydb.schema.Table;
import ch.unibas.dmi.dbis.polyphenydb.schema.impl.AbstractSchema;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.geode.cache.GemFireCache;
import org.apache.geode.cache.Region;


/**
 * Schema mapped onto a Geode Region.
 */
public class GeodeSchema extends AbstractSchema {

    final GemFireCache cache;
    private final List<String> regionNames;
    private ImmutableMap<String, Table> tableMap;


    GeodeSchema( String locatorHost, int locatorPort, Iterable<String> regionNames, String pdxAutoSerializerPackageExp ) {
        this( GeodeUtils.createClientCache( locatorHost, locatorPort, pdxAutoSerializerPackageExp, true ), regionNames );
    }


    GeodeSchema( final GemFireCache cache, final Iterable<String> regionNames ) {
        super();
        this.cache = Objects.requireNonNull( cache, "clientCache" );
        this.regionNames = ImmutableList.copyOf( Objects.requireNonNull( regionNames, "regionNames" ) );
    }


    @Override
    protected Map<String, Table> getTableMap() {
        if ( tableMap == null ) {
            final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
            for ( String regionName : regionNames ) {
                Region region = GeodeUtils.createRegion( cache, regionName );
                Table table = new GeodeTable( region );
                builder.put( regionName, table );
            }
            tableMap = builder.build();
        }
        return tableMap;
    }
}

