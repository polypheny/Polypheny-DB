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

package ch.unibas.dmi.dbis.polyphenydb.config;


import static org.apache.calcite.avatica.ConnectionConfigImpl.PropEnv;

import ch.unibas.dmi.dbis.polyphenydb.model.JsonSchema;
import ch.unibas.dmi.dbis.polyphenydb.sql.Lex;
import ch.unibas.dmi.dbis.polyphenydb.sql.NullCollation;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlConformanceEnum;
import ch.unibas.dmi.dbis.polyphenydb.util.Bug;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;
import org.apache.calcite.avatica.ConnectionProperty;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;


/**
 * Properties that may be specified on the JDBC connect string.
 */
public enum PolyphenyDbConnectionProperty implements ConnectionProperty {

    /**
     * Whether Polypheny-DB should use materializations.
     */
    MATERIALIZATIONS_ENABLED( "materializationsEnabled", Type.BOOLEAN, true, false ),

    /**
     * Whether Polypheny-DB should create materializations.
     */
    CREATE_MATERIALIZATIONS( "createMaterializations", Type.BOOLEAN, true, false ),

    /**
     * How NULL values should be sorted if neither NULLS FIRST nor NULLS LAST are specified. The default, HIGH, sorts NULL values the same as Oracle.
     */
    DEFAULT_NULL_COLLATION( "defaultNullCollation", Type.ENUM, NullCollation.HIGH, true, NullCollation.class ),

    /**
     * How many rows the Druid adapter should fetch at a time when executing "select" queries.
     */
    DRUID_FETCH( "druidFetch", Type.NUMBER, 16384, false ),

    /**
     * URI of the model.
     */
    MODEL( "model", Type.STRING, null, false ),

    /**
     * Lexical policy.
     */
    LEX( "lex", Type.ENUM, Lex.ORACLE, false ),

    /**
     * Collection of built-in functions and operators. Valid values include "standard", "oracle" and "spatial", and also comma-separated lists, for example "oracle,spatial".
     */
    FUN( "fun", Type.STRING, "standard", true ),

    /**
     * How identifiers are quoted. If not specified, value from {@link #LEX} is used.
     */
    QUOTING( "quoting", Type.ENUM, null, false, Quoting.class ),

    /**
     * How identifiers are stored if they are quoted. If not specified, value from {@link #LEX} is used.
     */
    QUOTED_CASING( "quotedCasing", Type.ENUM, null, false, Casing.class ),

    /**
     * How identifiers are stored if they are not quoted. If not specified, value from {@link #LEX} is used.
     */
    UNQUOTED_CASING( "unquotedCasing", Type.ENUM, null, false, Casing.class ),


    /**
     * Parser factory.
     *
     * The name of a class that implements {@link ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParserImplFactory}.
     */
    PARSER_FACTORY( "parserFactory", Type.PLUGIN, null, false ),

    /**
     * Name of initial schema.
     */
    SCHEMA( "schema", Type.STRING, null, false ),

    /**
     * Schema factory.
     *
     * The name of a class that implements {@link ch.unibas.dmi.dbis.polyphenydb.schema.SchemaFactory}.
     *
     * Ignored if {@link #MODEL} is specified.
     */
    SCHEMA_FACTORY( "schemaFactory", Type.PLUGIN, null, false ),

    /**
     * Schema type.
     *
     * Value may be null, "MAP", "JDBC", or "CUSTOM" (implicit if {@link #SCHEMA_FACTORY} is specified).
     *
     * Ignored if {@link #MODEL} is specified.
     */
    SCHEMA_TYPE( "schemaType", Type.ENUM, null, false, JsonSchema.Type.class ),

    /**
     * Returns the time zone from the connect string, for example 'gmt-3'. If the time zone is not set then the JVM time zone is returned.
     * Never null.
     */
    TIME_ZONE( "timeZone", Type.STRING, TimeZone.getDefault().getID(), false ),

    /**
     * If the planner should try de-correlating as much as it is possible. If true (the default), Polypheny-DB de-correlates the plan.
     */
    FORCE_DECORRELATE( "forceDecorrelate", Type.BOOLEAN, true, false ),

    /**
     * Type system. The name of a class that implements {@link ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeSystem} and has a public default constructor or an {@code INSTANCE} constant.
     */
    TYPE_SYSTEM( "typeSystem", Type.PLUGIN, null, false ),

    /**
     * SQL conformance level.
     */
    CONFORMANCE( "conformance", Type.ENUM, SqlConformanceEnum.DEFAULT, false );

    private final String camelName;
    private final Type type;
    private final Object defaultValue;
    private final boolean required;
    private final Class valueClass;

    private static final Map<String, PolyphenyDbConnectionProperty> NAME_TO_PROPS;

    /**
     * Deprecated; use {@link #TIME_ZONE}.
     */
    @Deprecated // to be removed before 2.0
    public static final PolyphenyDbConnectionProperty TIMEZONE = TIME_ZONE;


    static {
        NAME_TO_PROPS = new HashMap<>();
        for ( PolyphenyDbConnectionProperty p : PolyphenyDbConnectionProperty.values() ) {
            NAME_TO_PROPS.put( p.camelName.toUpperCase( Locale.ROOT ), p );
            NAME_TO_PROPS.put( p.name(), p );
        }
    }


    PolyphenyDbConnectionProperty( String camelName, Type type, Object defaultValue, boolean required ) {
        this( camelName, type, defaultValue, required, null );
    }


    PolyphenyDbConnectionProperty( String camelName, Type type, Object defaultValue, boolean required, Class valueClass ) {
        this.camelName = camelName;
        this.type = type;
        this.defaultValue = defaultValue;
        this.required = required;
        this.valueClass = type.deduceValueClass( defaultValue, valueClass );
        if ( !type.valid( defaultValue, this.valueClass ) ) {
            throw new AssertionError( camelName );
        }
    }


    public String camelName() {
        return camelName;
    }


    public Object defaultValue() {
        return defaultValue;
    }


    public Type type() {
        return type;
    }


    public Class valueClass() {
        return valueClass;
    }


    public boolean required() {
        return required;
    }


    public PropEnv wrap( Properties properties ) {
        return new PropEnv( parse2( properties, NAME_TO_PROPS ), this );
    }


    /**
     * Fixed version of {@link org.apache.calcite.avatica.ConnectionConfigImpl#parse} until we upgrade Avatica.
     */
    private static Map<ConnectionProperty, String> parse2( Properties properties, Map<String, ? extends ConnectionProperty> nameToProps ) {
        Bug.upgrade( "avatica-1.10" );
        final Map<ConnectionProperty, String> map = new LinkedHashMap<>();
        for ( String name : properties.stringPropertyNames() ) {
            final ConnectionProperty connectionProperty = nameToProps.get( name.toUpperCase( Locale.ROOT ) );
            if ( connectionProperty == null ) {
                // For now, don't throw. It messes up sub-projects.
                //throw new RuntimeException("Unknown property '" + name + "'");
                continue;
            }
            map.put( connectionProperty, properties.getProperty( name ) );
        }
        return map;
    }

}

