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

package ch.unibas.dmi.dbis.polyphenydb.sql.type;


import ch.unibas.dmi.dbis.polyphenydb.runtime.GeoFunctions;
import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import org.apache.calcite.avatica.util.ArrayImpl;


/**
 * JavaToSqlTypeConversionRules defines mappings from common Java types to corresponding SQL types.
 */
public class JavaToSqlTypeConversionRules {

    private static final JavaToSqlTypeConversionRules INSTANCE = new JavaToSqlTypeConversionRules();

    private final Map<Class<?>, SqlTypeName> rules =
            ImmutableMap.<Class<?>, SqlTypeName>builder()
                    .put( Integer.class, SqlTypeName.INTEGER )
                    .put( int.class, SqlTypeName.INTEGER )
                    .put( Long.class, SqlTypeName.BIGINT )
                    .put( long.class, SqlTypeName.BIGINT )
                    .put( Short.class, SqlTypeName.SMALLINT )
                    .put( short.class, SqlTypeName.SMALLINT )
                    .put( byte.class, SqlTypeName.TINYINT )
                    .put( Byte.class, SqlTypeName.TINYINT )

                    .put( Float.class, SqlTypeName.REAL )
                    .put( float.class, SqlTypeName.REAL )
                    .put( Double.class, SqlTypeName.DOUBLE )
                    .put( double.class, SqlTypeName.DOUBLE )

                    .put( boolean.class, SqlTypeName.BOOLEAN )
                    .put( Boolean.class, SqlTypeName.BOOLEAN )
                    .put( byte[].class, SqlTypeName.VARBINARY )
                    .put( String.class, SqlTypeName.VARCHAR )
                    .put( char[].class, SqlTypeName.VARCHAR )
                    .put( Character.class, SqlTypeName.CHAR )
                    .put( char.class, SqlTypeName.CHAR )

                    .put( java.util.Date.class, SqlTypeName.TIMESTAMP )
                    .put( Date.class, SqlTypeName.DATE )
                    .put( Timestamp.class, SqlTypeName.TIMESTAMP )
                    .put( Time.class, SqlTypeName.TIME )
                    .put( BigDecimal.class, SqlTypeName.DECIMAL )

                    .put( GeoFunctions.Geom.class, SqlTypeName.GEOMETRY )

                    .put( ResultSet.class, SqlTypeName.CURSOR )
                    .put( ColumnList.class, SqlTypeName.COLUMN_LIST )
                    .put( ArrayImpl.class, SqlTypeName.ARRAY )
                    .put( List.class, SqlTypeName.ARRAY )
                    .build();


    /**
     * Returns the {@link ch.unibas.dmi.dbis.polyphenydb.util.Glossary#SINGLETON_PATTERN singleton} instance.
     */
    public static JavaToSqlTypeConversionRules instance() {
        return INSTANCE;
    }


    /**
     * Returns a corresponding {@link SqlTypeName} for a given Java class.
     *
     * @param javaClass the Java class to lookup
     * @return a corresponding SqlTypeName if found, otherwise null is returned
     */
    public SqlTypeName lookup( Class javaClass ) {
        return rules.get( javaClass );
    }


    /**
     * Make this public when needed. To represent COLUMN_LIST SQL value, we need a type distinguishable from {@link List} in user-defined types.
     */
    private interface ColumnList extends List {

    }
}

