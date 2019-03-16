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

package ch.unibas.dmi.dbis.polyphenydb.sql.validate;


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeField;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Checks whether two names are the same according to a case-sensitivity policy.
 *
 * @see SqlNameMatchers
 */
public interface SqlNameMatcher {

    /**
     * Returns whether name matching is case-sensitive.
     */
    boolean isCaseSensitive();

    /**
     * Returns a name matches another.
     *
     * @param string Name written in code
     * @param name Name of object we are trying to match
     * @return Whether matches
     */
    boolean matches( String string, String name );

    /**
     * Looks up an item in a map.
     */
    <K extends List<String>, V> V get( Map<K, V> map, List<String> prefixNames, List<String> names );

    /**
     * Returns the most recent match.
     *
     * In the default implementation, throws {@link UnsupportedOperationException}.
     */
    String bestString();

    /**
     * Finds a field with a given name, using the current case-sensitivity, returning null if not found.
     *
     * @param rowType Row type
     * @param fieldName Field name
     * @return Field, or null if not found
     */
    RelDataTypeField field( RelDataType rowType, String fieldName );

    /**
     * Returns how many times a string occurs in a collection.
     *
     * Similar to {@link java.util.Collections#frequency}.
     */
    int frequency( Iterable<String> names, String name );

    /**
     * Creates a set that has the same case-sensitivity as this matcher.
     */
    Set<String> createSet();
}

