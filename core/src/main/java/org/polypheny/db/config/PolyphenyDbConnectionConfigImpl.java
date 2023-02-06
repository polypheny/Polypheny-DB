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

package org.polypheny.db.config;


import java.util.Properties;
import org.apache.calcite.avatica.ConnectionConfigImpl;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.polypheny.db.algebra.constant.ConformanceEnum;
import org.polypheny.db.algebra.constant.Lex;
import org.polypheny.db.algebra.constant.NullCollation;
import org.polypheny.db.util.Conformance;


/**
 * Implementation of {@link PolyphenyDbConnectionConfig}.
 */
public class PolyphenyDbConnectionConfigImpl extends ConnectionConfigImpl implements PolyphenyDbConnectionConfig {

    public PolyphenyDbConnectionConfigImpl( Properties properties ) {
        super( properties );
    }


    /**
     * Returns a copy of this configuration with one property changed.
     */
    public PolyphenyDbConnectionConfigImpl set( PolyphenyDbConnectionProperty property, String value ) {
        final Properties properties1 = new Properties( properties );
        properties1.setProperty( property.camelName(), value );
        return new PolyphenyDbConnectionConfigImpl( properties1 );
    }


    @Override
    public NullCollation defaultNullCollation() {
        return PolyphenyDbConnectionProperty.DEFAULT_NULL_COLLATION.wrap( properties ).getEnum( NullCollation.class, NullCollation.HIGH );
    }


    @Override
    public String model() {
        return PolyphenyDbConnectionProperty.MODEL.wrap( properties ).getString();
    }


    @Override
    public Lex lex() {
        return PolyphenyDbConnectionProperty.LEX.wrap( properties ).getEnum( Lex.class );
    }


    @Override
    public Quoting quoting() {
        return PolyphenyDbConnectionProperty.QUOTING.wrap( properties ).getEnum( Quoting.class, lex().quoting );
    }


    @Override
    public Casing unquotedCasing() {
        return PolyphenyDbConnectionProperty.UNQUOTED_CASING.wrap( properties ).getEnum( Casing.class, lex().unquotedCasing );
    }


    @Override
    public Casing quotedCasing() {
        return PolyphenyDbConnectionProperty.QUOTED_CASING.wrap( properties ).getEnum( Casing.class, lex().quotedCasing );
    }


    @Override
    public <T> T parserFactory( Class<T> parserFactoryClass, T defaultParserFactory ) {
        return PolyphenyDbConnectionProperty.PARSER_FACTORY.wrap( properties ).getPlugin( parserFactoryClass, defaultParserFactory );
    }


    @Override
    public boolean forceDecorrelate() {
        return PolyphenyDbConnectionProperty.FORCE_DECORRELATE.wrap( properties ).getBoolean();
    }


    @Override
    public <T> T typeSystem( Class<T> typeSystemClass, T defaultTypeSystem ) {
        return PolyphenyDbConnectionProperty.TYPE_SYSTEM.wrap( properties ).getPlugin( typeSystemClass, defaultTypeSystem );
    }


    @Override
    public Conformance conformance() {
        return PolyphenyDbConnectionProperty.CONFORMANCE.wrap( properties ).getEnum( ConformanceEnum.class );
    }


    @Override
    public String timeZone() {
        return PolyphenyDbConnectionProperty.TIME_ZONE.wrap( properties ).getString();
    }

}
