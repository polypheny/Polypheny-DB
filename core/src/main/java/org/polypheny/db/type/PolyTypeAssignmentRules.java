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


import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;


/**
 * Rules that determine whether a type is assignable from another type.
 */
public class PolyTypeAssignmentRules {

    private static final PolyTypeAssignmentRules INSTANCE;
    private static final PolyTypeAssignmentRules COERCE_INSTANCE;

    private final Map<PolyType, ImmutableSet<PolyType>> map;


    private PolyTypeAssignmentRules( Map<PolyType, ImmutableSet<PolyType>> map ) {
        this.map = ImmutableMap.copyOf( map );
    }


    static {
        final Builder rules = new Builder();

        final Set<PolyType> rule = new HashSet<>();

        // IntervalYearMonth is assignable from...
        for ( PolyType interval : PolyType.YEAR_INTERVAL_TYPES ) {
            rules.add( interval, PolyType.YEAR_INTERVAL_TYPES );
        }
        for ( PolyType interval : PolyType.DAY_INTERVAL_TYPES ) {
            rules.add( interval, PolyType.DAY_INTERVAL_TYPES );
        }
        for ( PolyType interval : PolyType.DAY_INTERVAL_TYPES ) {
            final Set<PolyType> dayIntervalTypes = PolyType.DAY_INTERVAL_TYPES;
            rules.add( interval, dayIntervalTypes );
        }

        // MULTISET is assignable from...
        rules.add( PolyType.MULTISET, EnumSet.of( PolyType.MULTISET ) );

        // TINYINT is assignable from...
        rules.add( PolyType.TINYINT, EnumSet.of( PolyType.TINYINT ) );

        // SMALLINT is assignable from...
        rule.clear();
        rule.add( PolyType.TINYINT );
        rule.add( PolyType.SMALLINT );
        rules.add( PolyType.SMALLINT, rule );

        // INTEGER is assignable from...
        rule.clear();
        rule.add( PolyType.TINYINT );
        rule.add( PolyType.SMALLINT );
        rule.add( PolyType.INTEGER );
        rules.add( PolyType.INTEGER, rule );

        // BIGINT is assignable from...
        rule.clear();
        rule.add( PolyType.TINYINT );
        rule.add( PolyType.SMALLINT );
        rule.add( PolyType.INTEGER );
        rule.add( PolyType.BIGINT );
        rules.add( PolyType.BIGINT, rule );

        // FLOAT (up to 64 bit floating point) is assignable from...
        rule.clear();
        rule.add( PolyType.TINYINT );
        rule.add( PolyType.SMALLINT );
        rule.add( PolyType.INTEGER );
        rule.add( PolyType.BIGINT );
        rule.add( PolyType.DECIMAL );
        rule.add( PolyType.FLOAT );
        rules.add( PolyType.FLOAT, rule );

        // REAL (32 bit floating point) is assignable from...
        rule.clear();
        rule.add( PolyType.TINYINT );
        rule.add( PolyType.SMALLINT );
        rule.add( PolyType.INTEGER );
        rule.add( PolyType.BIGINT );
        rule.add( PolyType.DECIMAL );
        rule.add( PolyType.FLOAT );
        rule.add( PolyType.REAL );
        rules.add( PolyType.REAL, rule );

        // DOUBLE is assignable from...
        rule.clear();
        rule.add( PolyType.TINYINT );
        rule.add( PolyType.SMALLINT );
        rule.add( PolyType.INTEGER );
        rule.add( PolyType.BIGINT );
        rule.add( PolyType.DECIMAL );
        rule.add( PolyType.FLOAT );
        rule.add( PolyType.REAL );
        rule.add( PolyType.DOUBLE );
        rules.add( PolyType.DOUBLE, rule );

        // DECIMAL is assignable from...
        rule.clear();
        rule.add( PolyType.TINYINT );
        rule.add( PolyType.SMALLINT );
        rule.add( PolyType.INTEGER );
        rule.add( PolyType.BIGINT );
        rule.add( PolyType.REAL );
        rule.add( PolyType.DOUBLE );
        rule.add( PolyType.DECIMAL );
        rules.add( PolyType.DECIMAL, rule );

        // VARBINARY is assignable from...
        rule.clear();
        rule.add( PolyType.VARBINARY );
        rule.add( PolyType.BINARY );
        rules.add( PolyType.VARBINARY, rule );

        // CHAR is assignable from...
        rules.add( PolyType.CHAR, EnumSet.of( PolyType.CHAR ) );

        // VARCHAR is assignable from...
        rule.clear();
        rule.add( PolyType.CHAR );
        rule.add( PolyType.VARCHAR );
        rules.add( PolyType.VARCHAR, rule );

        // BOOLEAN is assignable from...
        rules.add( PolyType.BOOLEAN, EnumSet.of( PolyType.BOOLEAN ) );

        // BINARY is assignable from...
        rule.clear();
        rule.add( PolyType.BINARY );
        rule.add( PolyType.VARBINARY );
        rules.add( PolyType.BINARY, rule );

        // FILE is assignable from...
        rule.clear();
        rule.add( PolyType.IMAGE );
        rule.add( PolyType.VIDEO );
        rule.add( PolyType.SOUND );
        rules.add( PolyType.FILE, rule );

        // DATE is assignable from...
        rule.clear();
        rule.add( PolyType.DATE );
        rule.add( PolyType.TIMESTAMP );
        rules.add( PolyType.DATE, rule );

        // TIME is assignable from...
        rule.clear();
        rule.add( PolyType.TIME );
        rule.add( PolyType.TIMESTAMP );
        rules.add( PolyType.TIME, rule );

        // TIME WITH LOCAL TIME ZONE is assignable from...
        rules.add( PolyType.TIME_WITH_LOCAL_TIME_ZONE, EnumSet.of( PolyType.TIME_WITH_LOCAL_TIME_ZONE ) );

        // TIMESTAMP is assignable from ...
        rules.add( PolyType.TIMESTAMP, EnumSet.of( PolyType.TIMESTAMP ) );

        // TIMESTAMP WITH LOCAL TIME ZONE is assignable from...
        rules.add( PolyType.TIMESTAMP_WITH_LOCAL_TIME_ZONE, EnumSet.of( PolyType.TIMESTAMP_WITH_LOCAL_TIME_ZONE ) );

        // GEOMETRY is assignable from ...
        rules.add( PolyType.GEOMETRY, EnumSet.of( PolyType.GEOMETRY ) );

        // ARRAY is assignable from ...
        rules.add( PolyType.ARRAY, EnumSet.of( PolyType.ARRAY ) );

        // ANY is assignable from ...
        rule.clear();
        rule.add( PolyType.TINYINT );
        rule.add( PolyType.SMALLINT );
        rule.add( PolyType.INTEGER );
        rule.add( PolyType.BIGINT );
        rule.add( PolyType.DECIMAL );
        rule.add( PolyType.FLOAT );
        rule.add( PolyType.REAL );
        rule.add( PolyType.TIME );
        rule.add( PolyType.DATE );
        rule.add( PolyType.TIMESTAMP );
        rules.add( PolyType.ANY, rule );

        // we use coerceRules when we're casting
        final Builder coerceRules = new Builder( rules );

        // Make numbers symmetrical, and make VARCHAR and CHAR castable to/from numbers
        rule.clear();
        rule.add( PolyType.TINYINT );
        rule.add( PolyType.SMALLINT );
        rule.add( PolyType.INTEGER );
        rule.add( PolyType.BIGINT );
        rule.add( PolyType.DECIMAL );
        rule.add( PolyType.FLOAT );
        rule.add( PolyType.REAL );
        rule.add( PolyType.DOUBLE );

        rule.add( PolyType.CHAR );
        rule.add( PolyType.VARCHAR );

        coerceRules.add( PolyType.TINYINT, rule );
        coerceRules.add( PolyType.SMALLINT, rule );
        coerceRules.add( PolyType.INTEGER, rule );
        coerceRules.add( PolyType.BIGINT, rule );
        coerceRules.add( PolyType.FLOAT, rule );
        coerceRules.add( PolyType.REAL, rule );
        coerceRules.add( PolyType.DECIMAL, rule );
        coerceRules.add( PolyType.DOUBLE, rule );
        coerceRules.add( PolyType.CHAR, rule );
        coerceRules.add( PolyType.VARCHAR, rule );

        // Exact numeric types are castable from intervals
        for ( PolyType exactType : PolyType.EXACT_TYPES ) {
            coerceRules.add(
                    exactType,
                    coerceRules.copyValues( exactType ).addAll( PolyType.INTERVAL_TYPES ).build() );
        }

        // Intervals are castable from exact numeric
        for ( PolyType typeName : PolyType.INTERVAL_TYPES ) {
            coerceRules.add(
                    typeName,
                    coerceRules.copyValues( typeName )
                            .add( PolyType.TINYINT )
                            .add( PolyType.SMALLINT )
                            .add( PolyType.INTEGER )
                            .add( PolyType.BIGINT )
                            .add( PolyType.DECIMAL )
                            .add( PolyType.VARCHAR )
                            .build() );
        }

        // VARCHAR is castable from BOOLEAN, DATE, TIMESTAMP, numeric types and intervals
        coerceRules.add(
                PolyType.VARCHAR,
                coerceRules.copyValues( PolyType.VARCHAR )
                        .add( PolyType.BOOLEAN )
                        .add( PolyType.DATE )
                        .add( PolyType.TIME )
                        .add( PolyType.TIMESTAMP )
                        .addAll( PolyType.INTERVAL_TYPES )
                        .build() );

        // CHAR is castable from BOOLEAN, DATE, TIME, TIMESTAMP and numeric types
        coerceRules.add(
                PolyType.CHAR,
                coerceRules.copyValues( PolyType.CHAR )
                        .add( PolyType.BOOLEAN )
                        .add( PolyType.DATE )
                        .add( PolyType.TIME )
                        .add( PolyType.TIMESTAMP )
                        .addAll( PolyType.INTERVAL_TYPES )
                        .build() );

        // BOOLEAN is castable from CHAR and VARCHAR
        coerceRules.add(
                PolyType.BOOLEAN,
                coerceRules.copyValues( PolyType.BOOLEAN )
                        .add( PolyType.CHAR )
                        .add( PolyType.VARCHAR )
                        .build() );

        // DATE, TIME, and TIMESTAMP are castable from CHAR and VARCHAR.

        // DATE is castable from...
        coerceRules.add(
                PolyType.DATE,
                coerceRules.copyValues( PolyType.DATE )
                        .add( PolyType.DATE )
                        .add( PolyType.TIMESTAMP )
                        .add( PolyType.TIMESTAMP_WITH_LOCAL_TIME_ZONE )
                        .add( PolyType.CHAR )
                        .add( PolyType.VARCHAR )
                        .build() );

        // TIME is castable from...
        coerceRules.add(
                PolyType.TIME,
                coerceRules.copyValues( PolyType.TIME )
                        .add( PolyType.TIME )
                        .add( PolyType.TIME_WITH_LOCAL_TIME_ZONE )
                        .add( PolyType.TIMESTAMP )
                        .add( PolyType.TIMESTAMP_WITH_LOCAL_TIME_ZONE )
                        .add( PolyType.CHAR )
                        .add( PolyType.VARCHAR )
                        .build() );

        // TIME WITH LOCAL TIME ZONE is castable from...
        coerceRules.add(
                PolyType.TIME_WITH_LOCAL_TIME_ZONE,
                coerceRules.copyValues( PolyType.TIME_WITH_LOCAL_TIME_ZONE )
                        .add( PolyType.TIME )
                        .add( PolyType.TIME_WITH_LOCAL_TIME_ZONE )
                        .add( PolyType.TIMESTAMP )
                        .add( PolyType.TIMESTAMP_WITH_LOCAL_TIME_ZONE )
                        .add( PolyType.CHAR )
                        .add( PolyType.VARCHAR )
                        .build() );

        // TIMESTAMP is castable from...
        coerceRules.add(
                PolyType.TIMESTAMP,
                coerceRules.copyValues( PolyType.TIMESTAMP )
                        .add( PolyType.TIMESTAMP )
                        .add( PolyType.TIMESTAMP_WITH_LOCAL_TIME_ZONE )
                        .add( PolyType.DATE )
                        .add( PolyType.TIME )
                        .add( PolyType.TIME_WITH_LOCAL_TIME_ZONE )
                        .add( PolyType.CHAR )
                        .add( PolyType.VARCHAR )
                        .build() );

        // TIMESTAMP WITH LOCAL TIME ZONE is castable from...
        coerceRules.add(
                PolyType.TIMESTAMP_WITH_LOCAL_TIME_ZONE,
                coerceRules.copyValues( PolyType.TIMESTAMP_WITH_LOCAL_TIME_ZONE )
                        .add( PolyType.TIMESTAMP )
                        .add( PolyType.TIMESTAMP_WITH_LOCAL_TIME_ZONE )
                        .add( PolyType.DATE )
                        .add( PolyType.TIME )
                        .add( PolyType.TIME_WITH_LOCAL_TIME_ZONE )
                        .add( PolyType.CHAR )
                        .add( PolyType.VARCHAR )
                        .build() );

        INSTANCE = new PolyTypeAssignmentRules( rules.map );
        COERCE_INSTANCE = new PolyTypeAssignmentRules( coerceRules.map );
    }


    /**
     * Returns an instance that does not coerce.
     */
    public static synchronized PolyTypeAssignmentRules instance() {
        return instance( false );
    }


    /**
     * Returns an instance.
     */
    public static synchronized PolyTypeAssignmentRules instance( boolean coerce ) {
        return coerce ? COERCE_INSTANCE : INSTANCE;
    }


    /**
     * Returns whether it is valid to cast a value of from type {@code from} to type {@code to}.
     */
    public boolean canCastFrom( PolyType to, PolyType from ) {
        Objects.requireNonNull( to );
        Objects.requireNonNull( from );

        if ( to == PolyType.NULL ) {
            return false;
        } else if ( from == PolyType.NULL ) {
            return true;
        }

        final Set<PolyType> rule = map.get( to );
        if ( rule == null ) {
            // if you hit this assert, see the constructor of this class on how to add new rule
            throw new AssertionError( "No assign rules for " + to + " defined" );
        }

        return rule.contains( from );
    }


    /**
     * Keeps state while maps are building build.
     */
    private static class Builder {

        final Map<PolyType, ImmutableSet<PolyType>> map;
        final LoadingCache<Set<PolyType>, ImmutableSet<PolyType>> sets;


        /**
         * Creates an empty Builder.
         */
        Builder() {
            this.map = new HashMap<>();
            this.sets = CacheBuilder.newBuilder().build( CacheLoader.from( set -> Sets.immutableEnumSet( set ) ) );
        }


        /**
         * Creates a Builder as a copy of another Builder.
         */
        Builder( Builder builder ) {
            this.map = new HashMap<>( builder.map );
            this.sets = builder.sets; // share the same canonical sets
        }


        void add( PolyType fromType, Set<PolyType> toTypes ) {
            try {
                map.put( fromType, sets.get( toTypes ) );
            } catch ( ExecutionException e ) {
                throw new RuntimeException( "populating SqlTypeAssignmentRules", e );
            }
        }


        ImmutableSet.Builder<PolyType> copyValues( PolyType typeName ) {
            return ImmutableSet.<PolyType>builder().addAll( map.get( typeName ) );
        }

    }

}

