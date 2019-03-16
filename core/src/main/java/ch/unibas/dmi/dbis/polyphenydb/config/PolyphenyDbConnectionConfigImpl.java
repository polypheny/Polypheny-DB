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


import ch.unibas.dmi.dbis.polyphenydb.model.JsonSchema.Type;
import ch.unibas.dmi.dbis.polyphenydb.prepare.PolyphenyDbCatalogReader;
import ch.unibas.dmi.dbis.polyphenydb.runtime.GeoFunctions;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.fun.OracleSqlOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.fun.SqlStdOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.util.ChainedSqlOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlConformance;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlConformanceEnum;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Properties;
import org.apache.calcite.avatica.ConnectionConfigImpl;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;


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


    public boolean approximateDistinctCount() {
        return PolyphenyDbConnectionProperty.APPROXIMATE_DISTINCT_COUNT.wrap( properties ).getBoolean();
    }


    public boolean approximateTopN() {
        return PolyphenyDbConnectionProperty.APPROXIMATE_TOP_N.wrap( properties ).getBoolean();
    }


    public boolean approximateDecimal() {
        return PolyphenyDbConnectionProperty.APPROXIMATE_DECIMAL.wrap( properties ).getBoolean();
    }


    @Override
    public boolean nullEqualToEmpty() {
        return PolyphenyDbConnectionProperty.NULL_EQUAL_TO_EMPTY.wrap( properties ).getBoolean();
    }


    public boolean autoTemp() {
        return PolyphenyDbConnectionProperty.AUTO_TEMP.wrap( properties ).getBoolean();
    }


    public boolean materializationsEnabled() {
        return PolyphenyDbConnectionProperty.MATERIALIZATIONS_ENABLED.wrap( properties ).getBoolean();
    }


    public boolean createMaterializations() {
        return PolyphenyDbConnectionProperty.CREATE_MATERIALIZATIONS.wrap( properties ).getBoolean();
    }


    public NullCollation defaultNullCollation() {
        return PolyphenyDbConnectionProperty.DEFAULT_NULL_COLLATION.wrap( properties ).getEnum( NullCollation.class, NullCollation.HIGH );
    }


    public <T> T fun( Class<T> operatorTableClass, T defaultOperatorTable ) {
        final String fun = PolyphenyDbConnectionProperty.FUN.wrap( properties ).getString();
        if ( fun == null || fun.equals( "" ) || fun.equals( "standard" ) ) {
            return defaultOperatorTable;
        }
        final Collection<SqlOperatorTable> tables = new LinkedHashSet<>();
        for ( String s : fun.split( "," ) ) {
            operatorTable( s, tables );
        }
        tables.add( SqlStdOperatorTable.instance() );
        return operatorTableClass.cast( ChainedSqlOperatorTable.of( tables.toArray( new SqlOperatorTable[0] ) ) );
    }


    private static void operatorTable( String s, Collection<SqlOperatorTable> tables ) {
        switch ( s ) {
            case "standard":
                tables.add( SqlStdOperatorTable.instance() );
                return;
            case "oracle":
                tables.add( OracleSqlOperatorTable.instance() );
                return;
            case "spatial":
                tables.add( PolyphenyDbCatalogReader.operatorTable( GeoFunctions.class.getName() ) );
                return;
            default:
                throw new IllegalArgumentException( "Unknown operator table: " + s );
        }
    }


    public String model() {
        return PolyphenyDbConnectionProperty.MODEL.wrap( properties ).getString();
    }


    public Lex lex() {
        return PolyphenyDbConnectionProperty.LEX.wrap( properties ).getEnum( Lex.class );
    }


    public Quoting quoting() {
        return PolyphenyDbConnectionProperty.QUOTING.wrap( properties ).getEnum( Quoting.class, lex().quoting );
    }


    public Casing unquotedCasing() {
        return PolyphenyDbConnectionProperty.UNQUOTED_CASING.wrap( properties ).getEnum( Casing.class, lex().unquotedCasing );
    }


    public Casing quotedCasing() {
        return PolyphenyDbConnectionProperty.QUOTED_CASING.wrap( properties ).getEnum( Casing.class, lex().quotedCasing );
    }


    public boolean caseSensitive() {
        return PolyphenyDbConnectionProperty.CASE_SENSITIVE.wrap( properties ).getBoolean( lex().caseSensitive );
    }


    public <T> T parserFactory( Class<T> parserFactoryClass, T defaultParserFactory ) {
        return PolyphenyDbConnectionProperty.PARSER_FACTORY.wrap( properties ).getPlugin( parserFactoryClass, defaultParserFactory );
    }


    public <T> T schemaFactory( Class<T> schemaFactoryClass, T defaultSchemaFactory ) {
        return PolyphenyDbConnectionProperty.SCHEMA_FACTORY.wrap( properties ).getPlugin( schemaFactoryClass, defaultSchemaFactory );
    }


    public Type schemaType() {
        return PolyphenyDbConnectionProperty.SCHEMA_TYPE.wrap( properties ).getEnum( Type.class );
    }


    public boolean spark() {
        return PolyphenyDbConnectionProperty.SPARK.wrap( properties ).getBoolean();
    }


    public boolean forceDecorrelate() {
        return PolyphenyDbConnectionProperty.FORCE_DECORRELATE.wrap( properties ).getBoolean();
    }


    public <T> T typeSystem( Class<T> typeSystemClass, T defaultTypeSystem ) {
        return PolyphenyDbConnectionProperty.TYPE_SYSTEM.wrap( properties ).getPlugin( typeSystemClass, defaultTypeSystem );
    }


    public SqlConformance conformance() {
        return PolyphenyDbConnectionProperty.CONFORMANCE.wrap( properties ).getEnum( SqlConformanceEnum.class );
    }


    @Override
    public String timeZone() {
        return PolyphenyDbConnectionProperty.TIME_ZONE.wrap( properties ).getString();
    }
}
