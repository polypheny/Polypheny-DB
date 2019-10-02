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
 *  The MIT License (MIT)
 *
 *  Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a
 *  copy of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.sql.test;


import ch.unibas.dmi.dbis.polyphenydb.jdbc.JavaTypeFactoryImpl;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.DelegatingTypeSystem;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeFactory;
import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataTypeSystem;
import ch.unibas.dmi.dbis.polyphenydb.sql.SqlOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.advise.SqlAdvisor;
import ch.unibas.dmi.dbis.polyphenydb.sql.fun.SqlStdOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParser;
import ch.unibas.dmi.dbis.polyphenydb.sql.parser.SqlParser.Config;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlConformance;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlConformanceEnum;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidator;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorCatalogReader;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorUtil;
import ch.unibas.dmi.dbis.polyphenydb.sql.validate.SqlValidatorWithHints;
import ch.unibas.dmi.dbis.polyphenydb.test.MockSqlOperatorTable;
import ch.unibas.dmi.dbis.polyphenydb.test.PolyphenyDbAssert;
import ch.unibas.dmi.dbis.polyphenydb.test.catalog.MockCatalogReader;
import ch.unibas.dmi.dbis.polyphenydb.test.catalog.MockCatalogReaderSimple;
import ch.unibas.dmi.dbis.polyphenydb.util.SourceStringReader;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import java.util.Map;
import java.util.Objects;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;


/**
 * Default implementation of {@link SqlTestFactory}.
 *
 * Suitable for most tests. If you want different behavior, you can extend; if you want a factory with different properties (e.g. SQL conformance level or identifier quoting), use {@link #with(String, Object)} to create a new factory.
 */
public class SqlTestFactory {

    public static final ImmutableMap<String, Object> DEFAULT_OPTIONS =
            ImmutableSortedMap.<String, Object>naturalOrder()
                    .put( "quoting", Quoting.DOUBLE_QUOTE )
                    .put( "quotedCasing", Casing.UNCHANGED )
                    .put( "unquotedCasing", Casing.TO_UPPER )
                    .put( "caseSensitive", true )
                    .put( "conformance", SqlConformanceEnum.DEFAULT )
                    .put( "operatorTable", SqlStdOperatorTable.instance() )
                    .put( "connectionFactory", PolyphenyDbAssert.EMPTY_CONNECTION_FACTORY.with( new PolyphenyDbAssert.AddSchemaSpecPostProcessor( PolyphenyDbAssert.SchemaSpec.HR ) ) )
                    .build();

    public static final SqlTestFactory INSTANCE = new SqlTestFactory();

    private final ImmutableMap<String, Object> options;
    private final MockCatalogReaderFactory catalogReaderFactory;
    private final ValidatorFactory validatorFactory;

    private final Supplier<RelDataTypeFactory> typeFactory;
    private final Supplier<SqlOperatorTable> operatorTable;
    private final Supplier<SqlValidatorCatalogReader> catalogReader;
    private final Supplier<Config> parserConfig;


    protected SqlTestFactory() {
        this( DEFAULT_OPTIONS, MockCatalogReaderSimple::new, SqlValidatorUtil::newValidator );
    }


    protected SqlTestFactory( ImmutableMap<String, Object> options, MockCatalogReaderFactory catalogReaderFactory, ValidatorFactory validatorFactory ) {
        this.options = options;
        this.catalogReaderFactory = catalogReaderFactory;
        this.validatorFactory = validatorFactory;
        this.operatorTable = Suppliers.memoize( () -> createOperatorTable( (SqlOperatorTable) options.get( "operatorTable" ) ) );
        this.typeFactory = Suppliers.memoize( () -> createTypeFactory( (SqlConformance) options.get( "conformance" ) ) );
        Boolean caseSensitive = (Boolean) options.get( "caseSensitive" );
        this.catalogReader = Suppliers.memoize( () -> catalogReaderFactory.create( typeFactory.get(), caseSensitive ).init() );
        this.parserConfig = Suppliers.memoize( () -> createParserConfig( options ) );
    }


    private static SqlOperatorTable createOperatorTable( SqlOperatorTable opTab0 ) {
        MockSqlOperatorTable opTab = new MockSqlOperatorTable( opTab0 );
        MockSqlOperatorTable.addRamp( opTab );
        return opTab;
    }


    public Config getParserConfig() {
        return parserConfig.get();
    }


    public SqlParser createParser( String sql ) {
        return SqlParser.create( new SourceStringReader( sql ), parserConfig.get() );
    }


    public static SqlParser.Config createParserConfig( ImmutableMap<String, Object> options ) {
        return SqlParser.configBuilder()
                .setQuoting( (Quoting) options.get( "quoting" ) )
                .setUnquotedCasing( (Casing) options.get( "unquotedCasing" ) )
                .setQuotedCasing( (Casing) options.get( "quotedCasing" ) )
                .setConformance( (SqlConformance) options.get( "conformance" ) )
                .setCaseSensitive( (boolean) options.get( "caseSensitive" ) )
                .build();
    }


    public SqlValidator getValidator() {
        final SqlConformance conformance = (SqlConformance) options.get( "conformance" );
        return validatorFactory.create( operatorTable.get(), catalogReader.get(), typeFactory.get(), conformance );
    }


    public SqlAdvisor createAdvisor() {
        SqlValidator validator = getValidator();
        if ( validator instanceof SqlValidatorWithHints ) {
            return new SqlAdvisor( (SqlValidatorWithHints) validator, parserConfig.get() );
        }
        throw new UnsupportedOperationException( "Validator should implement SqlValidatorWithHints, actual validator is " + validator );
    }


    public SqlTestFactory with( String name, Object value ) {
        if ( Objects.equals( value, options.get( name ) ) ) {
            return this;
        }
        ImmutableMap.Builder<String, Object> builder = ImmutableSortedMap.naturalOrder();
        // Protect from IllegalArgumentException: Multiple entries with same key
        for ( Map.Entry<String, Object> entry : options.entrySet() ) {
            if ( name.equals( entry.getKey() ) ) {
                continue;
            }
            builder.put( entry );
        }
        builder.put( name, value );
        return new SqlTestFactory( builder.build(), catalogReaderFactory, validatorFactory );
    }


    public SqlTestFactory withCatalogReader( MockCatalogReaderFactory newCatalogReaderFactory ) {
        return new SqlTestFactory( options, newCatalogReaderFactory, validatorFactory );
    }


    public SqlTestFactory withValidator( ValidatorFactory newValidatorFactory ) {
        return new SqlTestFactory( options, catalogReaderFactory, newValidatorFactory );
    }


    public final Object get( String name ) {
        return options.get( name );
    }


    private static RelDataTypeFactory createTypeFactory( SqlConformance conformance ) {
        RelDataTypeSystem typeSystem = RelDataTypeSystem.DEFAULT;
        if ( conformance.shouldConvertRaggedUnionTypesToVarying() ) {
            typeSystem = new DelegatingTypeSystem( typeSystem ) {
                @Override
                public boolean shouldConvertRaggedUnionTypesToVarying() {
                    return true;
                }
            };
        }
        if ( conformance.allowExtendedTrim() ) {
            typeSystem = new DelegatingTypeSystem( typeSystem ) {
                public boolean allowExtendedTrim() {
                    return true;
                }
            };
        }
        return new JavaTypeFactoryImpl( typeSystem );
    }


    /**
     * Creates {@link SqlValidator} for tests.
     */
    public interface ValidatorFactory {

        SqlValidator create( SqlOperatorTable opTab, SqlValidatorCatalogReader catalogReader, RelDataTypeFactory typeFactory, SqlConformance conformance );
    }


    /**
     * Creates {@link MockCatalogReader} for tests.
     * Note: {@link MockCatalogReader#init()} is to be invoked later, so a typical implementation should be via constructor reference like {@code MockCatalogReaderSimple::new}.
     */
    public interface MockCatalogReaderFactory {

        MockCatalogReader create( RelDataTypeFactory typeFactory, boolean caseSensitive );
    }
}

