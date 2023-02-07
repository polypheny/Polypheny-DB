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
 */

package org.polypheny.db.mql.mql;

import java.util.HashMap;
import java.util.Map;
import lombok.Getter;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.languages.NodeParseException;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.languages.mql.MqlNode;
import org.polypheny.db.languages.mql.parser.MqlParser;
import org.polypheny.db.languages.mql.parser.MqlParser.MqlParserConfig;
import org.polypheny.db.mql.MqlLanguageDependent;
import org.polypheny.db.mql.mql2alg.MqlMockCatalog;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.util.SourceStringReader;


public abstract class MqlTest extends MqlLanguageDependent {

    private static final MqlParserConfig parserConfig;
    @Getter
    private static final Map<String, Operator> biComparisons;
    @Getter
    private static final Map<String, Kind> logicalOperators;

    public static final Operator eq = OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.MQL_EQUALS );
    public static final Operator ne = OperatorRegistry.get( OperatorName.NOT_EQUALS );
    public static final Operator gt = OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.MQL_GT );
    public static final Operator gte = OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.MQL_GTE );
    public static final Operator lt = OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.MQL_LT );
    public static final Operator lte = OperatorRegistry.get( QueryLanguage.from( "mongo" ), OperatorName.MQL_LTE );


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

        logicalOperators.put( "$and", Kind.AND );
        logicalOperators.put( "$or", Kind.OR );
        // logicalOperators.put( "nor", Kind.N);
        // logicalOperators.put( "not", Kind.NOT );
    }


    public MqlNode parse( String mql ) {
        final MqlParser parser = MqlParser.create( new SourceStringReader( mql ), parserConfig );
        try {
            return parser.parseStmt();
        } catch ( NodeParseException e ) {
            throw new RuntimeException( e );
        }
    }

}
