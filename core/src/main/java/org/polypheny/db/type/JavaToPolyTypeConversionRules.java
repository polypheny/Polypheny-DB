/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.type;


import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import org.apache.calcite.avatica.util.ArrayImpl;
import org.polypheny.db.runtime.functions.GeoFunctions;


/**
 * JavaToPolyTypeConversionRules defines mappings from common Java types to corresponding PolyTypes.
 */
public class JavaToPolyTypeConversionRules {

    private static final JavaToPolyTypeConversionRules INSTANCE = new JavaToPolyTypeConversionRules();

    private final Map<Class<?>, PolyType> rules =
            ImmutableMap.<Class<?>, PolyType>builder()
                    .put( Integer.class, PolyType.INTEGER )
                    .put( int.class, PolyType.INTEGER )
                    .put( Long.class, PolyType.BIGINT )
                    .put( long.class, PolyType.BIGINT )
                    .put( Short.class, PolyType.SMALLINT )
                    .put( short.class, PolyType.SMALLINT )
                    .put( byte.class, PolyType.TINYINT )
                    .put( Byte.class, PolyType.TINYINT )

                    .put( Float.class, PolyType.REAL )
                    .put( float.class, PolyType.REAL )
                    .put( Double.class, PolyType.DOUBLE )
                    .put( double.class, PolyType.DOUBLE )

                    .put( boolean.class, PolyType.BOOLEAN )
                    .put( Boolean.class, PolyType.BOOLEAN )
                    .put( byte[].class, PolyType.VARBINARY )
                    .put( String.class, PolyType.VARCHAR )
                    .put( char[].class, PolyType.VARCHAR )
                    .put( Character.class, PolyType.CHAR )
                    .put( char.class, PolyType.CHAR )

                    .put( java.util.Date.class, PolyType.TIMESTAMP )
                    .put( Date.class, PolyType.DATE )
                    .put( Timestamp.class, PolyType.TIMESTAMP )
                    .put( Time.class, PolyType.TIME )
                    .put( BigDecimal.class, PolyType.DECIMAL )

                    .put( GeoFunctions.Geom.class, PolyType.GEOMETRY )

                    .put( ResultSet.class, PolyType.CURSOR )
                    .put( ColumnList.class, PolyType.COLUMN_LIST )
                    .put( ArrayImpl.class, PolyType.ARRAY )
                    .put( List.class, PolyType.ARRAY )
                    .build();


    /**
     * Returns the {@link org.polypheny.db.util.Glossary#SINGLETON_PATTERN singleton} instance.
     */
    public static JavaToPolyTypeConversionRules instance() {
        return INSTANCE;
    }


    /**
     * Returns a corresponding {@link PolyType} for a given Java class.
     *
     * @param javaClass the Java class to lookup
     * @return a corresponding PolyType if found, otherwise null is returned
     */
    public PolyType lookup( Class javaClass ) {
        return rules.get( javaClass );
    }


    /**
     * Make this public when needed. To represent COLUMN_LIST SQL value, we need a type distinguishable
     * from {@link List} in user-defined types.
     */
    private interface ColumnList extends List {

    }

}

