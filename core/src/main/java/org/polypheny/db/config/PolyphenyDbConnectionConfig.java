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


import org.apache.calcite.avatica.ConnectionConfig;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.polypheny.db.algebra.constant.Lex;
import org.polypheny.db.algebra.constant.NullCollation;
import org.polypheny.db.util.Conformance;

/**
 * Interface for reading connection properties within Polypheny-DB code. There is a method for every property.
 * At some point there will be similar config classes for system and statement properties.
 */
public interface PolyphenyDbConnectionConfig extends ConnectionConfig {

    /**
     * @see PolyphenyDbConnectionProperty#DEFAULT_NULL_COLLATION
     */
    NullCollation defaultNullCollation();

    /**
     * @see PolyphenyDbConnectionProperty#MODEL
     */
    String model();

    /**
     * @see PolyphenyDbConnectionProperty#LEX
     */
    Lex lex();

    /**
     * @see PolyphenyDbConnectionProperty#QUOTING
     */
    Quoting quoting();

    /**
     * @see PolyphenyDbConnectionProperty#UNQUOTED_CASING
     */
    Casing unquotedCasing();

    /**
     * @see PolyphenyDbConnectionProperty#QUOTED_CASING
     */
    Casing quotedCasing();

    /**
     * @see PolyphenyDbConnectionProperty#PARSER_FACTORY
     */
    <T> T parserFactory( Class<T> parserFactoryClass, T defaultParserFactory );

    /**
     * @see PolyphenyDbConnectionProperty#FORCE_DECORRELATE
     */
    boolean forceDecorrelate();

    /**
     * @see PolyphenyDbConnectionProperty#TYPE_SYSTEM
     */
    <T> T typeSystem( Class<T> typeSystemClass, T defaultTypeSystem );

    /**
     * @see PolyphenyDbConnectionProperty#CONFORMANCE
     */
    Conformance conformance();

    /**
     * @see PolyphenyDbConnectionProperty#TIME_ZONE
     */
    @Override
    String timeZone();

}

