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

package org.polypheny.db.cypher;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.LangFunctionOperator;
import org.polypheny.db.nodes.Operator;

public class CypherRegisterer {

    @Getter
    @VisibleForTesting
    private static boolean isInit = false;


    public static void registerOperators() {
        if ( isInit ) {
            throw new RuntimeException( "Cypher operators were already registered." );
        }

        register( OperatorName.CYPHER_PROPERTIES_MATCH, new LangFunctionOperator( "CYPHER_PROPERTIES_MATCH", Kind.DESERIALIZE ) );

        register( OperatorName.CYPHER_LABELS_MATCH, new LangFunctionOperator( "CYPHER_LABELS_MATCH", Kind.DESERIALIZE ) );

        register( OperatorName.CYPHER_PATH_MATCH, new LangFunctionOperator( "CYPHER_PATH_MATCH", Kind.DESERIALIZE ) );

        register( OperatorName.CYPHER_NODE_EXTRACT, new LangFunctionOperator( "CYPHER_NODE_EXTRACT", Kind.DESERIALIZE ) );

        register( OperatorName.CYPHER_EXTRACT_FROM_PATH, new LangFunctionOperator( "CYPHER_EXTRACT_FROM_PATH", Kind.DESERIALIZE ) );

        isInit = true;
    }


    private static void register( OperatorName name, Operator operator ) {
        OperatorRegistry.register( QueryLanguage.CYPHER, name, operator );
    }

}
