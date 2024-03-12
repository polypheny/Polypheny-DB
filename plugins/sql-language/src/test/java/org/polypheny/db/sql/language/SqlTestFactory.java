/*
 * Copyright 2019-2024 The Polypheny Project
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
import org.polypheny.db.languages.Parser;
import org.polypheny.db.languages.Parser.ParserConfig;
import org.polypheny.db.prepare.JavaTypeFactoryImpl;
import org.polypheny.db.sql.MockSqlOperatorTable;
import org.polypheny.db.sql.language.fun.OracleSqlOperatorTable;
import org.polypheny.db.sql.language.parser.SqlAbstractParserImpl;
import org.polypheny.db.sql.language.parser.SqlParser;
import org.polypheny.db.sql.language.validate.SqlValidator;
import org.polypheny.db.sql.language.validate.SqlValidatorUtil;
import org.polypheny.db.util.Conformance;
import org.polypheny.db.util.SourceStringReader;


/**
 * Default implementation of {@link SqlTestFactory}.
 * <p>
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
                    .put( "operatorTable", OracleSqlOperatorTable.instance() )
                    .build();

    public static final SqlTestFactory INSTANCE = new SqlTestFactory();

    private final ImmutableMap<String, Object> options;
    private final ValidatorFactory validatorFactory;

    private final Supplier<AlgDataTypeFactory> typeFactory;
    private final Supplier<OperatorTable> operatorTable;
    private final Supplier<ParserConfig> parserConfig;


    protected SqlTestFactory() {
        this( DEFAULT_OPTIONS, SqlValidatorUtil::newValidator );
    }


    protected SqlTestFactory( ImmutableMap<String, Object> options, ValidatorFactory validatorFactory ) {
        this.options = options;
        this.validatorFactory = validatorFactory;
        this.operatorTable = Suppliers.memoize( () -> createOperatorTable( (OperatorTable) options.get( "operatorTable" ) ) );
        this.typeFactory = Suppliers.memoize( () -> createTypeFactory( (Conformance) options.get( "conformance" ) ) );
        this.parserConfig = Suppliers.memoize( () -> createParserConfig( options ) );
    }


    private static OperatorTable createOperatorTable( OperatorTable opTab0 ) {
        MockSqlOperatorTable opTab = new MockSqlOperatorTable( opTab0 );
        MockSqlOperatorTable.addRamp( opTab );
        return opTab;
    }



    public Parser createParser( String sql ) {
        ParserConfig config = parserConfig.get();
        SqlAbstractParserImpl parser = (SqlAbstractParserImpl) config.parserFactory().getParser( new SourceStringReader( sql ) );
        return new SqlParser( parser, config );
    }


    public static ParserConfig createParserConfig( ImmutableMap<String, Object> options ) {
        return Parser.configBuilder()
                .setQuoting( (Quoting) options.get( "quoting" ) )
                .setUnquotedCasing( (Casing) options.get( "unquotedCasing" ) )
                .setQuotedCasing( (Casing) options.get( "quotedCasing" ) )
                .setConformance( (Conformance) options.get( "conformance" ) )
                .build();
    }


    public SqlValidator getValidator() {
        final Conformance conformance = (Conformance) options.get( "conformance" );
        return validatorFactory.create( operatorTable.get(), typeFactory.get(), conformance );
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
        return new SqlTestFactory( builder.build(), validatorFactory );
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
            };
        }
        return new JavaTypeFactoryImpl( typeSystem );
    }


    /**
     * Creates {@link SqlValidator} for tests.
     */
    public interface ValidatorFactory {

        SqlValidator create( OperatorTable opTab, AlgDataTypeFactory typeFactory, Conformance conformance );

    }


}

