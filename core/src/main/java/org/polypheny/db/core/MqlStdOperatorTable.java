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
import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlOperator;


/**
 * Default operations used for MQL.
 */
public interface MqlStdOperatorTable {
    // document model operators

    LangFunctionOperator DOC_EQ = new LangFunctionOperator( "DOC_EQ", SqlKind.EQUALS );

    LangFunctionOperator DOC_SIZE_MATCH = new LangFunctionOperator( "DOC_SIZE_MATCH", SqlKind.DOC_SIZE_MATCH );

    LangFunctionOperator DOC_JSON_MATCH = new LangFunctionOperator( "DOC_JSON_MATCH", SqlKind.EQUALS );

    LangFunctionOperator DOC_REGEX_MATCH = new LangFunctionOperator( "DOC_REGEX_MATCH", SqlKind.DOC_REGEX_MATCH );

    LangFunctionOperator DOC_TYPE_MATCH = new LangFunctionOperator( "DOC_TYPE_MATCH", SqlKind.DOC_TYPE_MATCH );

    LangFunctionOperator DOC_QUERY_VALUE = new LangFunctionOperator( "DOC_QUERY_VALUE", SqlKind.DOC_FIELD );

    LangFunctionOperator DOC_SLICE = new LangFunctionOperator( "DOC_SLICE", SqlKind.DOC_SLICE );

    LangFunctionOperator DOC_ITEM = new LangFunctionOperator( "DOC_ITEM", SqlKind.DOC_ITEM );

    LangFunctionOperator DOC_QUERY_EXCLUDE = new LangFunctionOperator( "DOC_QUERY_EXCLUDE", SqlKind.DOC_EXCLUDE );

    LangFunctionOperator DOC_ADD_FIELDS = new LangFunctionOperator( "DOC_ADD_FIELDS", SqlKind.DOC_UPDATE_ADD );

    LangFunctionOperator DOC_UPDATE_MIN = new LangFunctionOperator( "DOC_UPDATE_MIN", SqlKind.MIN );

    LangFunctionOperator DOC_UPDATE_MAX = new LangFunctionOperator( "DOC_UPDATE_MAX", SqlKind.MAX );

    LangFunctionOperator DOC_UPDATE_ADD_TO_SET = new LangFunctionOperator( "DOC_UPDATE_ADD_TO_SET", SqlKind.DOC_UPDATE_ADD );

    LangFunctionOperator DOC_UPDATE_RENAME = new LangFunctionOperator( "DOC_UPDATE_RENAME", SqlKind.DOC_UPDATE_RENAME );

    LangFunctionOperator DOC_UPDATE_REPLACE = new LangFunctionOperator( "DOC_UPDATE_REPLACE", SqlKind.DOC_UPDATE_REPLACE );

    LangFunctionOperator DOC_UPDATE_REMOVE = new LangFunctionOperator( "DOC_UPDATE_REMOVE", SqlKind.DOC_UPDATE_REMOVE );

    LangFunctionOperator DOC_UPDATE = new LangFunctionOperator( "DOC_UPDATE", SqlKind.DOC_UPDATE );

    LangFunctionOperator DOC_ELEM_MATCH = new LangFunctionOperator( "DOC_ELEM_MATCH", SqlKind.DOC_ELEM_MATCH );

    LangFunctionOperator DOC_UNWIND = new LangFunctionOperator( "DOC_UNWIND", SqlKind.DOC_UNWIND );

    LangFunctionOperator DOC_EXISTS = new LangFunctionOperator( "DOC_EXISTS", SqlKind.DOC_EXISTS );

    LangFunctionOperator DOC_LT = new LangFunctionOperator( "DOC_LT", SqlKind.LESS_THAN );

    LangFunctionOperator DOC_GT = new LangFunctionOperator( "DOC_GT", SqlKind.GREATER_THAN );

    LangFunctionOperator DOC_LTE = new LangFunctionOperator( "DOC_LTE", SqlKind.LESS_THAN_OR_EQUAL );

    LangFunctionOperator DOC_GTE = new LangFunctionOperator( "DOC_GTE", SqlKind.GREATER_THAN_OR_EQUAL );

    LangFunctionOperator DOC_JSONIZE = new LangFunctionOperator( "DOC_JSONIZE", SqlKind.DOC_JSONIZE);

    List<SqlOperator> DOC_OPERATORS = Arrays.asList(
            DOC_EQ,
            DOC_GT,
            DOC_GTE,
            DOC_LT,
            DOC_LTE
    );

}
