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

package org.polypheny.db.mql;

import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import org.polypheny.db.mql.parser.MqlParseException;
import org.polypheny.db.mql.parser.MqlParser;
import org.polypheny.db.mql.parser.MqlParser.MqlParserConfig;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.util.SourceStringReader;

public abstract class MqlTest {

    private static final MqlParserConfig parserConfig;
    @Getter
    private static final Map<String, SqlKind> biComparisons;
    @Getter
    private static final Map<String, SqlKind> logicalOpertors;


    static {
        MqlParser.ConfigBuilder configConfigBuilder = MqlParser.configBuilder();
        parserConfig = configConfigBuilder.build();
        biComparisons = new HashMap<>();

        biComparisons.put( "$eq", SqlKind.EQUALS );
        biComparisons.put( "$ne", SqlKind.NOT_EQUALS )
        biComparisons.put( "$gt", SqlKind.GREATER_THAN );
        biComparisons.put( "$gte", SqlKind.GREATER_THAN_OR_EQUAL );
        biComparisons.put( "$lt", SqlKind.LESS_THAN );
        biComparisons.put( "$lte", SqlKind.LESS_THAN_OR_EQUAL );

        logicalOpertors = new HashMap<>();

        logicalOpertors.put( "$and", SqlKind.AND );
        logicalOpertors.put( "$or", SqlKind.OR );
        //logicalOpertors.put( "nor", SqlKind.N);
        //logicalOpertors.put( "not", SqlKind.NOT );
    }


    public MqlNode parse( String mql ) {
        final MqlParser parser = MqlParser.create( new SourceStringReader( mql ), parserConfig );
        try {
            return parser.parseStmt();
        } catch ( MqlParseException e ) {
            throw new RuntimeException( e );
        }
    }

}
