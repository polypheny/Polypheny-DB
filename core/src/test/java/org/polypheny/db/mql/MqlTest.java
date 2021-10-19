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
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.mql.fun.MqlStdOperatorTable;
import org.polypheny.db.mql.parser.MqlParseException;
import org.polypheny.db.mql.parser.MqlParser;
import org.polypheny.db.mql.parser.MqlParser.MqlParserConfig;
import org.polypheny.db.mql2rel.MqlMockCatalog;
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlOperator;
import org.polypheny.db.sql.fun.SqlStdOperatorTable;
import org.polypheny.db.util.SourceStringReader;


public abstract class MqlTest {

    private static final MqlParserConfig parserConfig;
    @Getter
    private static final Map<String, SqlOperator> biComparisons;
    @Getter
    private static final Map<String, SqlKind> logicalOperators;

    public static final SqlOperator eq = MqlStdOperatorTable.DOC_EQ;
    public static final SqlOperator ne = SqlStdOperatorTable.NOT_EQUALS;
    public static final SqlOperator gt = MqlStdOperatorTable.DOC_GT;
    public static final SqlOperator gte = MqlStdOperatorTable.DOC_GTE;
    public static final SqlOperator lt = MqlStdOperatorTable.DOC_LT;
    public static final SqlOperator lte = MqlStdOperatorTable.DOC_LTE;


    static {
        Catalog.setAndGetInstance( new MqlMockCatalog() );
        MqlParser.ConfigBuilder configConfigBuilder = MqlParser.configBuilder();
        parserConfig = configConfigBuilder.build();
        biComparisons = new HashMap<>();

        biComparisons.put( "$eq", eq );
        biComparisons.put( "$ne", ne );
        biComparisons.put( "$gt", gt );
        biComparisons.put( "$gte", gte );
        biComparisons.put( "$lt", lt );
        biComparisons.put( "$lte", lte );

        logicalOperators = new HashMap<>();

        logicalOperators.put( "$and", SqlKind.AND );
        logicalOperators.put( "$or", SqlKind.OR );
        //logicalOperators.put( "nor", SqlKind.N);
        //logicalOperators.put( "not", SqlKind.NOT );
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
