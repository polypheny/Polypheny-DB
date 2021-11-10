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

package org.polypheny.db.core;

import java.util.Arrays;
import java.util.List;


/**
 * Default operations used for MQL.
 */
public interface MqlStdOperatorTable {
    // document model operators

    LangFunctionOperator DOC_EQ = new LangFunctionOperator( "DOC_EQ", Kind.EQUALS );

    LangFunctionOperator DOC_SIZE_MATCH = new LangFunctionOperator( "DOC_SIZE_MATCH", Kind.DOC_SIZE_MATCH );

    LangFunctionOperator DOC_JSON_MATCH = new LangFunctionOperator( "DOC_JSON_MATCH", Kind.EQUALS );

    LangFunctionOperator DOC_REGEX_MATCH = new LangFunctionOperator( "DOC_REGEX_MATCH", Kind.DOC_REGEX_MATCH );

    LangFunctionOperator DOC_TYPE_MATCH = new LangFunctionOperator( "DOC_TYPE_MATCH", Kind.DOC_TYPE_MATCH );

    LangFunctionOperator DOC_QUERY_VALUE = new LangFunctionOperator( "DOC_QUERY_VALUE", Kind.DOC_FIELD );

    LangFunctionOperator DOC_SLICE = new LangFunctionOperator( "DOC_SLICE", Kind.DOC_SLICE );

    LangFunctionOperator DOC_ITEM = new LangFunctionOperator( "DOC_ITEM", Kind.DOC_ITEM );

    LangFunctionOperator DOC_QUERY_EXCLUDE = new LangFunctionOperator( "DOC_QUERY_EXCLUDE", Kind.DOC_EXCLUDE );

    LangFunctionOperator DOC_ADD_FIELDS = new LangFunctionOperator( "DOC_ADD_FIELDS", Kind.DOC_UPDATE_ADD );

    LangFunctionOperator DOC_UPDATE_MIN = new LangFunctionOperator( "DOC_UPDATE_MIN", Kind.MIN );

    LangFunctionOperator DOC_UPDATE_MAX = new LangFunctionOperator( "DOC_UPDATE_MAX", Kind.MAX );

    LangFunctionOperator DOC_UPDATE_ADD_TO_SET = new LangFunctionOperator( "DOC_UPDATE_ADD_TO_SET", Kind.DOC_UPDATE_ADD );

    LangFunctionOperator DOC_UPDATE_RENAME = new LangFunctionOperator( "DOC_UPDATE_RENAME", Kind.DOC_UPDATE_RENAME );

    LangFunctionOperator DOC_UPDATE_REPLACE = new LangFunctionOperator( "DOC_UPDATE_REPLACE", Kind.DOC_UPDATE_REPLACE );

    LangFunctionOperator DOC_UPDATE_REMOVE = new LangFunctionOperator( "DOC_UPDATE_REMOVE", Kind.DOC_UPDATE_REMOVE );

    LangFunctionOperator DOC_UPDATE = new LangFunctionOperator( "DOC_UPDATE", Kind.DOC_UPDATE );

    LangFunctionOperator DOC_ELEM_MATCH = new LangFunctionOperator( "DOC_ELEM_MATCH", Kind.DOC_ELEM_MATCH );

    LangFunctionOperator DOC_UNWIND = new LangFunctionOperator( "DOC_UNWIND", Kind.DOC_UNWIND );

    LangFunctionOperator DOC_EXISTS = new LangFunctionOperator( "DOC_EXISTS", Kind.DOC_EXISTS );

    LangFunctionOperator DOC_LT = new LangFunctionOperator( "DOC_LT", Kind.LESS_THAN );

    LangFunctionOperator DOC_GT = new LangFunctionOperator( "DOC_GT", Kind.GREATER_THAN );

    LangFunctionOperator DOC_LTE = new LangFunctionOperator( "DOC_LTE", Kind.LESS_THAN_OR_EQUAL );

    LangFunctionOperator DOC_GTE = new LangFunctionOperator( "DOC_GTE", Kind.GREATER_THAN_OR_EQUAL );

    LangFunctionOperator DOC_JSONIZE = new LangFunctionOperator( "DOC_JSONIZE", Kind.DOC_JSONIZE);

    List<Operator> DOC_OPERATORS = Arrays.asList(
            DOC_EQ,
            DOC_GT,
            DOC_GTE,
            DOC_LT,
            DOC_LTE
    );

}
