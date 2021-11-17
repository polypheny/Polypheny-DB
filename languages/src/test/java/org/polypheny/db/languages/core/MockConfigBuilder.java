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
 */

package org.polypheny.db.languages.core;

import com.google.common.collect.ImmutableList;
import java.io.Reader;
import org.polypheny.db.core.OperatorTable;
import org.polypheny.db.languages.NodeToRelConverter.Config;
import org.polypheny.db.languages.Parser;
import org.polypheny.db.languages.Parser.ParserConfig;
import org.polypheny.db.languages.RexConvertletTable;
import org.polypheny.db.languages.sql.fun.SqlRegisterer;
import org.polypheny.db.languages.sql.fun.SqlStdOperatorTable;
import org.polypheny.db.languages.sql.parser.SqlAbstractParserImpl;
import org.polypheny.db.languages.sql.parser.SqlParser;
import org.polypheny.db.languages.sql.parser.impl.SqlParserImpl;
import org.polypheny.db.languages.sql2rel.StandardConvertletTable;
import org.polypheny.db.rel.type.RelDataTypeSystem;
import org.polypheny.db.tools.Frameworks.ConfigBuilder;
import org.polypheny.db.tools.Frameworks.StdFrameworkConfig;
import org.polypheny.db.tools.Program;
import org.polypheny.db.util.SourceStringReader;

public class MockConfigBuilder {


    public static ConfigBuilder build() {
        new SqlRegisterer();
        RexConvertletTable convertletTable = StandardConvertletTable.INSTANCE;
        OperatorTable operatorTable = SqlStdOperatorTable.instance();
        ImmutableList<Program> programs = ImmutableList.of();
        ParserConfig parserConfig = mockParserConfig().build();
        Config sqlToRelConverterConfig = Config.DEFAULT;
        RelDataTypeSystem typeSystem = RelDataTypeSystem.DEFAULT;
        return new ConfigBuilder( new StdFrameworkConfig( null,
                convertletTable,
                operatorTable,
                programs,
                null,
                parserConfig,
                sqlToRelConverterConfig,
                null,
                null,
                typeSystem,
                null,
                null,
                null ) );
    }


    public static Parser.ConfigBuilder mockParserConfig() {
        new SqlRegisterer();
        return new Parser.ConfigBuilder( SqlParserImpl.FACTORY );
    }


    public static SqlParser createMockParser( String query ) {
        return createMockParser( query, mockParserConfig().build() );
    }


    public static SqlParser createMockParser( Reader reader ) {
        return createMockParser( reader, mockParserConfig().build() );
    }


    public static SqlParser createMockParser( String query, ParserConfig config ) {
        return createMockParser( new SourceStringReader( query ), config );
    }


    public static SqlParser createMockParser( Reader reader, ParserConfig config ) {
        new SqlRegisterer();
        SqlAbstractParserImpl parser = (SqlAbstractParserImpl) SqlParserImpl.FACTORY.getParser( reader );

        return new SqlParser( parser, config );
    }


}
