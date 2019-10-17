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


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypePrecedenceList;
import ch.unibas.dmi.dbis.polyphenydb.util.Glossary;
import ch.unibas.dmi.dbis.polyphenydb.util.ImmutableNullableList;
import ch.unibas.dmi.dbis.polyphenydb.util.Util;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;


/**
 * SqlTypeExplicitPrecedenceList implements the {@link RelDataTypePrecedenceList} interface via an explicit list of {@link SqlTypeName} entries.
 */
public class SqlTypeExplicitPrecedenceList implements RelDataTypePrecedenceList {

    // NOTE jvs 25-Jan-2005:  the null entries delimit equivalence classes
    private static final List<SqlTypeName> NUMERIC_TYPES =
            ImmutableNullableList.of(
                    SqlTypeName.TINYINT,
                    null,
                    SqlTypeName.SMALLINT,
                    null,
                    SqlTypeName.INTEGER,
                    null,
                    SqlTypeName.BIGINT,
                    null,
                    SqlTypeName.DECIMAL,
                    null,
                    SqlTypeName.REAL,
                    null,
                    SqlTypeName.FLOAT,
                    SqlTypeName.DOUBLE );

    private static final List<SqlTypeName> COMPACT_NUMERIC_TYPES = ImmutableList.copyOf( Util.filter( NUMERIC_TYPES, Objects::nonNull ) );

    /**
     * Map from SqlTypeName to corresponding precedence list.
     *
     * @see Glossary#SQL2003 SQL:2003 Part 2 Section 9.5
     */
    private static final Map<SqlTypeName, SqlTypeExplicitPrecedenceList> TYPE_NAME_TO_PRECEDENCE_LIST =
            ImmutableMap.<SqlTypeName, SqlTypeExplicitPrecedenceList>builder()
                    .put( SqlTypeName.BOOLEAN, list( SqlTypeName.BOOLEAN ) )
                    .put( SqlTypeName.TINYINT, numeric( SqlTypeName.TINYINT ) )
                    .put( SqlTypeName.SMALLINT, numeric( SqlTypeName.SMALLINT ) )
                    .put( SqlTypeName.INTEGER, numeric( SqlTypeName.INTEGER ) )
                    .put( SqlTypeName.BIGINT, numeric( SqlTypeName.BIGINT ) )
                    .put( SqlTypeName.DECIMAL, numeric( SqlTypeName.DECIMAL ) )
                    .put( SqlTypeName.REAL, numeric( SqlTypeName.REAL ) )
                    .put( SqlTypeName.FLOAT, list( SqlTypeName.FLOAT, SqlTypeName.REAL, SqlTypeName.DOUBLE ) )
                    .put( SqlTypeName.DOUBLE, list( SqlTypeName.DOUBLE, SqlTypeName.DECIMAL ) )
                    .put( SqlTypeName.CHAR, list( SqlTypeName.CHAR, SqlTypeName.VARCHAR ) )
                    .put( SqlTypeName.VARCHAR, list( SqlTypeName.VARCHAR ) )
                    .put( SqlTypeName.BINARY, list( SqlTypeName.BINARY, SqlTypeName.VARBINARY ) )
                    .put( SqlTypeName.VARBINARY, list( SqlTypeName.VARBINARY ) )
                    .put( SqlTypeName.DATE, list( SqlTypeName.DATE ) )
                    .put( SqlTypeName.TIME, list( SqlTypeName.TIME ) )
                    .put( SqlTypeName.TIMESTAMP, list( SqlTypeName.TIMESTAMP, SqlTypeName.DATE, SqlTypeName.TIME ) )
                    .put( SqlTypeName.INTERVAL_YEAR, list( SqlTypeName.YEAR_INTERVAL_TYPES ) )
                    .put( SqlTypeName.INTERVAL_YEAR_MONTH, list( SqlTypeName.YEAR_INTERVAL_TYPES ) )
                    .put( SqlTypeName.INTERVAL_MONTH, list( SqlTypeName.YEAR_INTERVAL_TYPES ) )
                    .put( SqlTypeName.INTERVAL_DAY, list( SqlTypeName.DAY_INTERVAL_TYPES ) )
                    .put( SqlTypeName.INTERVAL_DAY_HOUR, list( SqlTypeName.DAY_INTERVAL_TYPES ) )
                    .put( SqlTypeName.INTERVAL_DAY_MINUTE, list( SqlTypeName.DAY_INTERVAL_TYPES ) )
                    .put( SqlTypeName.INTERVAL_DAY_SECOND, list( SqlTypeName.DAY_INTERVAL_TYPES ) )
                    .put( SqlTypeName.INTERVAL_HOUR, list( SqlTypeName.DAY_INTERVAL_TYPES ) )
                    .put( SqlTypeName.INTERVAL_HOUR_MINUTE, list( SqlTypeName.DAY_INTERVAL_TYPES ) )
                    .put( SqlTypeName.INTERVAL_HOUR_SECOND, list( SqlTypeName.DAY_INTERVAL_TYPES ) )
                    .put( SqlTypeName.INTERVAL_MINUTE, list( SqlTypeName.DAY_INTERVAL_TYPES ) )
                    .put( SqlTypeName.INTERVAL_MINUTE_SECOND, list( SqlTypeName.DAY_INTERVAL_TYPES ) )
                    .put( SqlTypeName.INTERVAL_SECOND, list( SqlTypeName.DAY_INTERVAL_TYPES ) )
                    .build();


    private final List<SqlTypeName> typeNames;


    public SqlTypeExplicitPrecedenceList( Iterable<SqlTypeName> typeNames ) {
        this.typeNames = ImmutableNullableList.copyOf( typeNames );
    }


    private static SqlTypeExplicitPrecedenceList list( SqlTypeName... typeNames ) {
        return list( Arrays.asList( typeNames ) );
    }


    private static SqlTypeExplicitPrecedenceList list( Iterable<SqlTypeName> typeNames ) {
        return new SqlTypeExplicitPrecedenceList( typeNames );
    }


    private static SqlTypeExplicitPrecedenceList numeric( SqlTypeName typeName ) {
        int i = getListPosition( typeName, COMPACT_NUMERIC_TYPES );
        return new SqlTypeExplicitPrecedenceList( Util.skip( COMPACT_NUMERIC_TYPES, i ) );
    }


    // implement RelDataTypePrecedenceList
    @Override
    public boolean containsType( RelDataType type ) {
        SqlTypeName typeName = type.getSqlTypeName();
        return typeName != null && typeNames.contains( typeName );
    }


    // implement RelDataTypePrecedenceList
    @Override
    public int compareTypePrecedence( RelDataType type1, RelDataType type2 ) {
        assert containsType( type1 ) : type1;
        assert containsType( type2 ) : type2;

        int p1 = getListPosition( type1.getSqlTypeName(), typeNames );
        int p2 = getListPosition( type2.getSqlTypeName(), typeNames );
        return p2 - p1;
    }


    private static int getListPosition( SqlTypeName type, List<SqlTypeName> list ) {
        int i = list.indexOf( type );
        assert i != -1;

        // adjust for precedence equivalence classes
        for ( int j = i - 1; j >= 0; --j ) {
            if ( list.get( j ) == null ) {
                return j;
            }
        }
        return i;
    }


    static RelDataTypePrecedenceList getListForType( RelDataType type ) {
        SqlTypeName typeName = type.getSqlTypeName();
        if ( typeName == null ) {
            return null;
        }
        return TYPE_NAME_TO_PRECEDENCE_LIST.get( typeName );
    }
}

