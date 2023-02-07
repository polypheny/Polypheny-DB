/*
 * Copyright 2019-2023 The Polypheny Project
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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;
import org.polypheny.db.util.Source;

import java.io.IOException;
import java.util.List;


/**
 * Enumerator that reads from a JSON file.
 */
class JsonEnumerator implements Enumerator<Object[]> {

    private final Enumerator<Object> enumerator;


    JsonEnumerator( Source source ) {
        try {
            final ObjectMapper mapper = new ObjectMapper();
            mapper.configure( JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true );
            mapper.configure( JsonParser.Feature.ALLOW_SINGLE_QUOTES, true );
            mapper.configure( JsonParser.Feature.ALLOW_COMMENTS, true );
            List<Object> list;
            if ( source.protocol().equals( "file" ) ) {
                //noinspection unchecked
                list = mapper.readValue( source.file(), List.class );
            } else {
                //noinspection unchecked
                list = mapper.readValue( source.url(), List.class );
            }
            enumerator = Linq4j.enumerator( list );
        } catch ( IOException e ) {
            throw new RuntimeException( e );
        }
    }


    @Override
    public Object[] current() {
        return new Object[]{ enumerator.current() };
    }


    @Override
    public boolean moveNext() {
        return enumerator.moveNext();
    }


    @Override
    public void reset() {
        enumerator.reset();
    }


    @Override
    public void close() {
        try {
            enumerator.close();
        } catch ( Exception e ) {
            throw new RuntimeException( "Error closing JSON reader", e );
        }
    }

}

