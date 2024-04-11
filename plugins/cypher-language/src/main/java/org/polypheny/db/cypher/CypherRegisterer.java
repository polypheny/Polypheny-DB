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

package org.polypheny.db.cypher;

import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.nodes.LangFunctionOperator;
import org.polypheny.db.nodes.Operator;

public class CypherRegisterer {

    @Getter
    @VisibleForTesting
    private static boolean isInit = false;


    public static void registerOperators() {
        if ( isInit ) {
            throw new GenericRuntimeException( "Cypher operators were already registered." );
        }

        register( OperatorName.CYPHER_LIKE, new LangFunctionOperator( OperatorName.CYPHER_LIKE.name(), Kind.LIKE ) );

        register( OperatorName.CYPHER_HAS_PROPERTY, new LangFunctionOperator( OperatorName.CYPHER_HAS_PROPERTY.name(), Kind.CYPHER_FUNCTION ) );

        register( OperatorName.CYPHER_HAS_LABEL, new LangFunctionOperator( OperatorName.CYPHER_HAS_LABEL.name(), Kind.CYPHER_FUNCTION ) );

        register( OperatorName.CYPHER_GRAPH_ONLY_LABEL, new LangFunctionOperator( OperatorName.CYPHER_GRAPH_ONLY_LABEL.name(), Kind.CYPHER_FUNCTION ) );

        register( OperatorName.CYPHER_PATH_MATCH, new LangFunctionOperator( OperatorName.CYPHER_PATH_MATCH.name(), Kind.CYPHER_FUNCTION ) );

        register( OperatorName.CYPHER_NODE_EXTRACT, new LangFunctionOperator( OperatorName.CYPHER_NODE_EXTRACT.name(), Kind.CYPHER_FUNCTION ) );

        register( OperatorName.CYPHER_EXTRACT_FROM_PATH, new LangFunctionOperator( OperatorName.CYPHER_EXTRACT_FROM_PATH.name(), Kind.CYPHER_FUNCTION ) );

        register( OperatorName.CYPHER_NODE_MATCH, new LangFunctionOperator( OperatorName.CYPHER_NODE_MATCH.name(), Kind.CYPHER_FUNCTION ) );

        register( OperatorName.CYPHER_EXTRACT_PROPERTY, new LangFunctionOperator( OperatorName.CYPHER_EXTRACT_PROPERTY.name(), Kind.CYPHER_FUNCTION ) );

        register( OperatorName.CYPHER_EXTRACT_PROPERTIES, new LangFunctionOperator( OperatorName.CYPHER_EXTRACT_PROPERTIES.name(), Kind.CYPHER_FUNCTION ) );

        register( OperatorName.CYPHER_EXTRACT_ID, new LangFunctionOperator( OperatorName.CYPHER_EXTRACT_ID.name(), Kind.CYPHER_FUNCTION ) );

        register( OperatorName.CYPHER_EXTRACT_LABELS, new LangFunctionOperator( OperatorName.CYPHER_EXTRACT_LABELS.name(), Kind.CYPHER_FUNCTION ) );

        register( OperatorName.CYPHER_HAS_LABEL, new LangFunctionOperator( OperatorName.CYPHER_HAS_LABEL.name(), Kind.CYPHER_FUNCTION ) );

        register( OperatorName.CYPHER_TO_LIST, new LangFunctionOperator( OperatorName.CYPHER_TO_LIST.name(), Kind.CYPHER_FUNCTION ) );

        register( OperatorName.CYPHER_ADJUST_EDGE, new LangFunctionOperator( OperatorName.CYPHER_ADJUST_EDGE.name(), Kind.CYPHER_FUNCTION ) );

        register( OperatorName.CYPHER_SET_LABELS, new LangFunctionOperator( OperatorName.CYPHER_SET_LABELS.name(), Kind.CYPHER_FUNCTION ) );

        register( OperatorName.CYPHER_SET_PROPERTY, new LangFunctionOperator( OperatorName.CYPHER_SET_PROPERTY.name(), Kind.CYPHER_FUNCTION ) );

        register( OperatorName.CYPHER_SET_PROPERTIES, new LangFunctionOperator( OperatorName.CYPHER_SET_PROPERTIES.name(), Kind.CYPHER_FUNCTION ) );

        register( OperatorName.CYPHER_REMOVE_PROPERTY, new LangFunctionOperator( OperatorName.CYPHER_REMOVE_PROPERTY.name(), Kind.CYPHER_FUNCTION ) );

        register( OperatorName.CYPHER_REMOVE_LABELS, new LangFunctionOperator( OperatorName.CYPHER_REMOVE_LABELS.name(), Kind.CYPHER_FUNCTION ) );

        isInit = true;
    }


    private static void register( OperatorName name, Operator operator ) {
        OperatorRegistry.register( QueryLanguage.from( "cypher" ), name, operator );
    }


    public static void removeOperators() {
        OperatorRegistry.remove( QueryLanguage.from( "cypher" ) );
    }

}
