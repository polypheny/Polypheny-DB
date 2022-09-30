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

package org.polypheny.db.languages.core;


import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.catalog.Catalog.QueryLanguage;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.DeserializeFunctionOperator;
import org.polypheny.db.nodes.LangFunctionOperator;
import org.polypheny.db.nodes.Operator;


/**
 * Default operations used for MQL.
 */
public class MqlRegisterer {

    @Getter
    @VisibleForTesting
    private static boolean isInit = false;

    // document model operators


    public static void registerOperators() {
        if ( isInit ) {
            throw new RuntimeException( "Mql operators were already registered." );
        }
        register( OperatorName.MQL_EQUALS, new LangFunctionOperator( "MQL_EQUALS", Kind.EQUALS ) );

        register( OperatorName.MQL_SIZE_MATCH, new LangFunctionOperator( "MQL_SIZE_MATCH", Kind.MQL_SIZE_MATCH ) );

        register( OperatorName.MQL_JSON_MATCH, new LangFunctionOperator( "MQL_JSON_MATCH", Kind.EQUALS ) );

        register( OperatorName.MQL_REGEX_MATCH, new LangFunctionOperator( "MQL_REGEX_MATCH", Kind.MQL_REGEX_MATCH ) );

        register( OperatorName.MQL_TYPE_MATCH, new LangFunctionOperator( "MQL_TYPE_MATCH", Kind.MQL_TYPE_MATCH ) );

        register( OperatorName.MQL_QUERY_VALUE, new LangFunctionOperator( "MQL_QUERY_VALUE", Kind.MQL_QUERY_VALUE ) );

        register( OperatorName.MQL_SLICE, new LangFunctionOperator( "MQL_SLICE", Kind.MQL_SLICE ) );

        register( OperatorName.MQL_ITEM, new LangFunctionOperator( "MQL_ITEM", Kind.MQL_ITEM ) );

        register( OperatorName.MQL_EXCLUDE, new LangFunctionOperator( "MQL_EXCLUDE", Kind.MQL_EXCLUDE ) );

        register( OperatorName.MQL_ADD_FIELDS, new LangFunctionOperator( "MQL_ADD_FIELDS", Kind.MQL_ADD_FIELDS ) );

        register( OperatorName.MQL_UPDATE_MIN, new LangFunctionOperator( "MQL_UPDATE_MIN", Kind.MIN ) );

        register( OperatorName.MQL_UPDATE_MAX, new LangFunctionOperator( "MQL_UPDATE_MAX", Kind.MAX ) );

        register( OperatorName.MQL_UPDATE_ADD_TO_SET, new LangFunctionOperator( "MQL_UPDATE_ADD_TO_SET", Kind.MQL_ADD_FIELDS ) );

        register( OperatorName.MQL_UPDATE_RENAME, new LangFunctionOperator( "MQL_UPDATE_RENAME", Kind.MQL_UPDATE_RENAME ) );

        register( OperatorName.MQL_UPDATE_REPLACE, new LangFunctionOperator( "MQL_UPDATE_REPLACE", Kind.MQL_UPDATE_REPLACE ) );

        register( OperatorName.MQL_UPDATE_REMOVE, new LangFunctionOperator( "MQL_UPDATE_REMOVE", Kind.MQL_UPDATE_REMOVE ) );

        register( OperatorName.MQL_UPDATE, new LangFunctionOperator( "MQL_UPDATE", Kind.MQL_UPDATE ) );

        register( OperatorName.MQL_ELEM_MATCH, new LangFunctionOperator( "MQL_ELEM_MATCH", Kind.MQL_ELEM_MATCH ) );

        register( OperatorName.MQL_UNWIND, new LangFunctionOperator( "UNWIND", Kind.UNWIND ) );

        register( OperatorName.MQL_EXISTS, new LangFunctionOperator( "MQL_EXISTS", Kind.MQL_EXISTS ) );

        register( OperatorName.MQL_LT, new LangFunctionOperator( "MQL_LT", Kind.LESS_THAN ) );

        register( OperatorName.MQL_GT, new LangFunctionOperator( "MQL_GT", Kind.GREATER_THAN ) );

        register( OperatorName.MQL_LTE, new LangFunctionOperator( "MQL_LTE", Kind.LESS_THAN_OR_EQUAL ) );

        register( OperatorName.MQL_GTE, new LangFunctionOperator( "MQL_GTE", Kind.GREATER_THAN_OR_EQUAL ) );

        register( OperatorName.MQL_JSONIFY, new LangFunctionOperator( "MQL_JSONIFY", Kind.MQL_JSONIFY ) );

        register( OperatorName.DESERIALIZE, new DeserializeFunctionOperator( "DESERIALIZE_DOC" ) );

        isInit = true;
    }


    private static void register( OperatorName name, Operator operator ) {
        OperatorRegistry.register( QueryLanguage.MONGO_QL, name, operator );
    }


}
