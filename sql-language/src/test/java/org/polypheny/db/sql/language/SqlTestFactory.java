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
 */

package org.polypheny.db.sql.language;


import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import java.util.Map;
import java.util.Objects;
import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.avatica.util.Quoting;
import org.polypheny.db.algebra.constant.ConformanceEnum;
import org.polypheny.db.algebra.operators.OperatorTable;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeSystem;
import org.polypheny.db.algebra.type.DelegatingTypeSystem;
import org.polypheny.db.catalog.MockCatalogReader;
import org.polypheny.db.catalog.MockCatalogReaderSimple;
import org.polypheny.db.languages.LanguageManager;
import org.polypheny.db.languages.Parser;
import org.polypheny.db.languages.Parser.ParserConfig;
import org.polypheny.db.nodes.validate.ValidatorCatalogReader;
import org.polypheny.db.prepare.JavaTypeFactoryImpl;
import org.polypheny.db.sql.MockSqlOperatorTable;
import org.polypheny.db.sql.language.advise.SqlAdvisor;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.sql.language.validate.SqlValidatorUtil;
import org.polypheny.db.sql.language.validate.SqlValidatorWithHints;
import org.polypheny.db.util.Conformance;
import org.polypheny.db.util.SourceStringReader;


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
                    .put( "conformance", ConformanceEnum.DEFAULT )
                    .put( "operatorTable", LanguageManager.getInstance().getStdOperatorTable() )
                    .build();

    public static final SqlTestFactory INSTANCE = new SqlTestFactory();

    private final ImmutableMap<String, Object> options;
    private final MockCatalogReaderFactory catalogReaderFactory;
    private final ValidatorFactory validatorFactory;

    private final Supplier<AlgDataTypeFactory> typeFactory;
    private final Supplier<OperatorTable> operatorTable;
    private final Supplier<ValidatorCatalogReader> catalogReader;
    private final Supplier<ParserConfig> parserConfig;


    protected SqlTestFactory() {
        this( DEFAULT_OPTIONS, MockCatalogReaderSimple::new, SqlValidatorUtil::newValidator );
    }


    protected SqlTestFactory( ImmutableMap<String, Object> options, MockCatalogReaderFactory catalogReaderFactory, ValidatorFactory validatorFactory ) {
        this.options = options;
        this.catalogReaderFactory = catalogReaderFactory;
        this.validatorFactory = validatorFactory;
        this.operatorTable = Suppliers.memoize( () -> createOperatorTable( (OperatorTable) options.get( "operatorTable" ) ) );
        this.typeFactory = Suppliers.memoize( () -> createTypeFactory( (Conformance) options.get( "conformance" ) ) );
        Boolean caseSensitive = (Boolean) options.get( "caseSensitive" );
        this.catalogReader = Suppliers.memoize( () -> catalogReaderFactory.create( typeFactory.get(), caseSensitive ).init() );
        this.parserConfig = Suppliers.memoize( () -> createParserConfig( options ) );
    }


    private static OperatorTable createOperatorTable( OperatorTable opTab0 ) {
        MockSqlOperatorTable opTab = new MockSqlOperatorTable( opTab0 );
        MockSqlOperatorTable.addRamp( opTab );
        return opTab;
    }


    public ParserConfig getParserConfig() {
        return parserConfig.get();
    }


    public Parser createParser( String sql ) {
        return Parser.create( new SourceStringReader( sql ), parserConfig.get() );
    }


    public static ParserConfig createParserConfig( ImmutableMap<String, Object> options ) {
        return Parser.configBuilder()
                .setQuoting( (Quoting) options.get( "quoting" ) )
                .setUnquotedCasing( (Casing) options.get( "unquotedCasing" ) )
                .setQuotedCasing( (Casing) options.get( "quotedCasing" ) )
                .setConformance( (Conformance) options.get( "conformance" ) )
                .setCaseSensitive( (boolean) options.get( "caseSensitive" ) )
                .build();
    }


    public SqlValidator getValidator() {
        final Conformance conformance = (Conformance) options.get( "conformance" );
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


    private static AlgDataTypeFactory createTypeFactory( Conformance conformance ) {
        AlgDataTypeSystem typeSystem = AlgDataTypeSystem.DEFAULT;
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

        SqlValidator create( OperatorTable opTab, ValidatorCatalogReader catalogReader, AlgDataTypeFactory typeFactory, Conformance conformance );

    }


    /**
     * Creates {@link MockCatalogReader} for tests.
     * Note: {@link MockCatalogReader#init()} is to be invoked later, so a typical implementation should be via constructor reference like {@code MockCatalogReaderSimple::new}.
     */
    public interface MockCatalogReaderFactory {

        MockCatalogReader create( AlgDataTypeFactory typeFactory, boolean caseSensitive );

    }

}

