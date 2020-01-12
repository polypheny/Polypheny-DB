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


import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.geode.cache.Region;
import org.apache.geode.pdx.PdxInstance;
import org.apache.geode.pdx.PdxInstanceFactory;


/**
 * Populates a geode region from a file having JSON entries (line by line).
 */
class JsonLoader {

    private static final String ROOT_PACKATE = "ch.unibas.dmi.dbis.polyphenydb.adapter.geode";

    private final String rootPackage;
    private final Region region;
    private final ObjectMapper mapper;


    JsonLoader( Region<?, ?> region ) {
        this.region = Objects.requireNonNull( region, "region" );
        this.rootPackage = ROOT_PACKATE;
        this.mapper = new ObjectMapper();
    }


    private void load( Reader reader ) throws IOException {
        Objects.requireNonNull( reader, "reader" );
        try ( BufferedReader br = new BufferedReader( reader ) ) {
            List<Map<String, Object>> mapList = new ArrayList<>();
            for ( String line; (line = br.readLine()) != null; ) {
                @SuppressWarnings("unchecked")
                Map<String, Object> jsonMap = mapper.readValue( line, Map.class );
                mapList.add( jsonMap );
            }
            loadMapList( mapList );
        }
    }


    void loadMapList( List<Map<String, Object>> mapList ) {
        int key = 0;
        for ( Map<String, Object> jsonMap : mapList ) {
            PdxInstance pdxInstance = mapToPdx( rootPackage, jsonMap );
            region.put( key++, pdxInstance );
        }
    }


    void loadClasspathResource( String location ) throws IOException {
        Objects.requireNonNull( location, "location" );
        InputStream is = getClass().getResourceAsStream( location );
        if ( is == null ) {
            throw new IllegalArgumentException( "Resource " + location + " not found in the classpath" );
        }

        load( new InputStreamReader( is, StandardCharsets.UTF_8 ) );
    }


    private PdxInstance mapToPdx( String packageName, Map<String, Object> map ) {
        PdxInstanceFactory pdxBuilder = region.getRegionService().createPdxInstanceFactory( packageName );

        for ( String name : map.keySet() ) {
            Object value = map.get( name );

            if ( value instanceof Map ) {
                pdxBuilder.writeObject( name, mapToPdx( packageName + "." + name, (Map) value ) );
            } else {
                pdxBuilder.writeObject( name, value );
            }
        }

        return pdxBuilder.create();
    }

}

