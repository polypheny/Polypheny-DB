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


import ch.unibas.dmi.dbis.polyphenydb.model.JsonSchema;
import ch.unibas.dmi.dbis.polyphenydb.sql.Lex;
import ch.unibas.dmi.dbis.polyphenydb.sql.NullCollation;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlConformance;
import org.apache.calcite.avatica.ConnectionConfig;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;


/**
 * Interface for reading connection properties within Polypheny-DB code. There is a method for every property. At some point there will be similar config classes for system and statement properties.
 */
public interface PolyphenyDbConnectionConfig extends ConnectionConfig {

    /**
     * @see PolyphenyDbConnectionProperty#MATERIALIZATIONS_ENABLED
     */
    boolean materializationsEnabled();

    /**
     * @see PolyphenyDbConnectionProperty#CREATE_MATERIALIZATIONS
     */
    boolean createMaterializations();

    /**
     * @see PolyphenyDbConnectionProperty#DEFAULT_NULL_COLLATION
     */
    NullCollation defaultNullCollation();

    /**
     * @see PolyphenyDbConnectionProperty#FUN
     */
    <T> T fun( Class<T> operatorTableClass, T defaultOperatorTable );

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
     * @see PolyphenyDbConnectionProperty#SCHEMA_FACTORY
     */
    <T> T schemaFactory( Class<T> schemaFactoryClass, T defaultSchemaFactory );

    /**
     * @see PolyphenyDbConnectionProperty#SCHEMA_TYPE
     */
    JsonSchema.Type schemaType();

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
    SqlConformance conformance();

    /**
     * @see PolyphenyDbConnectionProperty#TIME_ZONE
     */
    @Override
    String timeZone();
}

