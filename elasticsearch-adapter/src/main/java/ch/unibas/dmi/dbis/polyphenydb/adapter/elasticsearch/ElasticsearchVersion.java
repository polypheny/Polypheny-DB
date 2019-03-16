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


import java.util.Locale;
import java.util.Objects;


/**
 * Identifies current ES version at runtime. Some queries have different syntax depending on version (eg. 2 vs 5).
 */
enum ElasticsearchVersion {

    ES2,
    ES5,
    ES6,
    ES7,
    UNKNOWN;


    static ElasticsearchVersion fromString( String version ) {
        Objects.requireNonNull( version, "version" );
        if ( !version.matches( "\\d+\\.\\d+\\.\\d+" ) ) {
            final String message = String.format( Locale.ROOT, "Wrong version format. Expected ${digit}.${digit}.${digit} but got %s", version );
            throw new IllegalArgumentException( message );
        }

        // version format is: major.minor.revision
        final int major = Integer.parseInt( version.substring( 0, version.indexOf( "." ) ) );
        if ( major == 2 ) {
            return ES2;
        } else if ( major == 5 ) {
            return ES5;
        } else if ( major == 6 ) {
            return ES6;
        } else if ( major == 7 ) {
            return ES7;
        } else {
            return UNKNOWN;
        }
    }
}

