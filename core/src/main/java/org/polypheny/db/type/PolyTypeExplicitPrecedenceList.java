/*
 * Copyright 2019-2021 The Polypheny Project
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


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypePrecedenceList;
import org.polypheny.db.util.Glossary;
import org.polypheny.db.util.ImmutableNullableList;
import org.polypheny.db.util.Util;


/**
 * SqlTypeExplicitPrecedenceList implements the {@link AlgDataTypePrecedenceList} interface via an explicit
 * list of {@link PolyType} entries.
 */
public class PolyTypeExplicitPrecedenceList implements AlgDataTypePrecedenceList {

    // NOTE: The null entries delimit equivalence classes
    private static final List<PolyType> NUMERIC_TYPES =
            ImmutableNullableList.of(
                    PolyType.TINYINT,
                    null,
                    PolyType.SMALLINT,
                    null,
                    PolyType.INTEGER,
                    null,
                    PolyType.BIGINT,
                    null,
                    PolyType.DECIMAL,
                    null,
                    PolyType.REAL,
                    null,
                    PolyType.FLOAT,
                    PolyType.DOUBLE );

    private static final List<PolyType> COMPACT_NUMERIC_TYPES = ImmutableList.copyOf( Util.filter( NUMERIC_TYPES, Objects::nonNull ) );

    /**
     * Map from PolyType to corresponding precedence list.
     *
     * @see Glossary#SQL2003 SQL:2003 Part 2 Section 9.5
     */
    private static final Map<PolyType, PolyTypeExplicitPrecedenceList> TYPE_NAME_TO_PRECEDENCE_LIST =
            ImmutableMap.<PolyType, PolyTypeExplicitPrecedenceList>builder()
                    .put( PolyType.BOOLEAN, list( PolyType.BOOLEAN ) )
                    .put( PolyType.TINYINT, numeric( PolyType.TINYINT ) )
                    .put( PolyType.SMALLINT, numeric( PolyType.SMALLINT ) )
                    .put( PolyType.INTEGER, numeric( PolyType.INTEGER ) )
                    .put( PolyType.BIGINT, numeric( PolyType.BIGINT ) )
                    .put( PolyType.DECIMAL, numeric( PolyType.DECIMAL ) )
                    .put( PolyType.REAL, numeric( PolyType.REAL ) )
                    .put( PolyType.FLOAT, list( PolyType.FLOAT, PolyType.REAL, PolyType.DOUBLE ) )
                    .put( PolyType.DOUBLE, list( PolyType.DOUBLE, PolyType.DECIMAL ) )
                    .put( PolyType.CHAR, list( PolyType.CHAR, PolyType.VARCHAR ) )
                    .put( PolyType.VARCHAR, list( PolyType.VARCHAR ) )
                    .put( PolyType.BINARY, list( PolyType.BINARY, PolyType.VARBINARY ) )
                    .put( PolyType.VARBINARY, list( PolyType.VARBINARY ) )
                    .put( PolyType.DATE, list( PolyType.DATE ) )
                    .put( PolyType.TIME, list( PolyType.TIME ) )
                    .put( PolyType.TIMESTAMP, list( PolyType.TIMESTAMP, PolyType.DATE, PolyType.TIME ) )
                    .put( PolyType.INTERVAL, list( PolyType.INTERVAL_TYPES ) )
                    .build();


    private final List<PolyType> typeNames;


    public PolyTypeExplicitPrecedenceList( Iterable<PolyType> typeNames ) {
        this.typeNames = ImmutableNullableList.copyOf( typeNames );
    }


    private static PolyTypeExplicitPrecedenceList list( PolyType... typeNames ) {
        return list( Arrays.asList( typeNames ) );
    }


    private static PolyTypeExplicitPrecedenceList list( Iterable<PolyType> typeNames ) {
        return new PolyTypeExplicitPrecedenceList( typeNames );
    }


    private static PolyTypeExplicitPrecedenceList numeric( PolyType typeName ) {
        int i = getListPosition( typeName, COMPACT_NUMERIC_TYPES );
        return new PolyTypeExplicitPrecedenceList( Util.skip( COMPACT_NUMERIC_TYPES, i ) );
    }


    // implement RelDataTypePrecedenceList
    @Override
    public boolean containsType( AlgDataType type ) {
        PolyType typeName = type.getPolyType();
        return typeName != null && typeNames.contains( typeName );
    }


    // implement RelDataTypePrecedenceList
    @Override
    public int compareTypePrecedence( AlgDataType type1, AlgDataType type2 ) {
        assert containsType( type1 ) : type1;
        assert containsType( type2 ) : type2;

        int p1 = getListPosition( type1.getPolyType(), typeNames );
        int p2 = getListPosition( type2.getPolyType(), typeNames );
        return p2 - p1;
    }


    private static int getListPosition( PolyType type, List<PolyType> list ) {
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


    static AlgDataTypePrecedenceList getListForType( AlgDataType type ) {
        PolyType typeName = type.getPolyType();
        if ( typeName == null ) {
            return null;
        }
        return TYPE_NAME_TO_PRECEDENCE_LIST.get( typeName );
    }

}

