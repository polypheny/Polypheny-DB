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

package org.polypheny.db.mql.fun;

import org.polypheny.db.sql.SqlKind;
import org.polypheny.db.sql.SqlOperator;


/**
 * Default operations used for MQL.
 */
public interface MqlStdOperatorTable {
    // document model operators

    MqlFunctionOperator DOC_EQ = new MqlFunctionOperator( "DOC_EQ", SqlKind.EQUALS );

    MqlFunctionOperator DOC_SIZE_MATCH = new MqlFunctionOperator( "DOC_SIZE_MATCH", SqlKind.DOC_SIZE_MATCH );

    MqlFunctionOperator DOC_JSON_MATCH = new MqlFunctionOperator( "DOC_JSON_MATCH", SqlKind.EQUALS );

    MqlFunctionOperator DOC_REGEX_MATCH = new MqlFunctionOperator( "DOC_REGEX_MATCH", SqlKind.DOC_REGEX_MATCH );

    MqlFunctionOperator DOC_TYPE_MATCH = new MqlFunctionOperator( "DOC_TYPE_MATCH", SqlKind.DOC_TYPE_MATCH );

    MqlFunctionOperator DOC_QUERY_VALUE = new MqlFunctionOperator( "DOC_QUERY_VALUE", SqlKind.DOC_VALUE );

    MqlFunctionOperator DOC_SLICE = new MqlFunctionOperator( "DOC_SLICE", SqlKind.DOC_SLICE );

    MqlFunctionOperator DOC_ITEM = new MqlFunctionOperator( "DOC_ITEM", SqlKind.DOC_ITEM );

    MqlFunctionOperator DOC_QUERY_EXCLUDE = new MqlFunctionOperator( "DOC_QUERY_EXCLUDE", SqlKind.DOC_EXCLUDE );

    MqlFunctionOperator DOC_ADD_FIELDS = new MqlFunctionOperator( "DOC_ADD_FIELDS", SqlKind.DOC_UPDATE_ADD );

    MqlFunctionOperator DOC_UPDATE_MIN = new MqlFunctionOperator( "DOC_UPDATE_MIN", SqlKind.MIN );

    MqlFunctionOperator DOC_UPDATE_MAX = new MqlFunctionOperator( "DOC_UPDATE_MAX", SqlKind.MAX );

    MqlFunctionOperator DOC_UPDATE_ADD_TO_SET = new MqlFunctionOperator( "DOC_UPDATE_ADD_TO_SET", SqlKind.DOC_UPDATE_ADD );

    MqlFunctionOperator DOC_UPDATE_RENAME = new MqlFunctionOperator( "DOC_UPDATE_RENAME", SqlKind.DOC_UPDATE_RENAME );

    MqlFunctionOperator DOC_UPDATE_REPLACE = new MqlFunctionOperator( "DOC_UPDATE_REPLACE", SqlKind.DOC_UPDATE_REPLACE );

    MqlFunctionOperator DOC_UPDATE_REMOVE = new MqlFunctionOperator( "DOC_UPDATE_REMOVE", SqlKind.DOC_UPDATE_REMOVE );

    MqlFunctionOperator DOC_UPDATE = new MqlFunctionOperator( "DOC_UPDATE", SqlKind.DOC_UPDATE );

    MqlFunctionOperator DOC_ELEM_MATCH = new MqlFunctionOperator( "DOC_ELEM_MATCH", SqlKind.DOC_ELEM_MATCH );

    MqlFunctionOperator DOC_UNWIND = new MqlFunctionOperator( "DOC_UNWIND", SqlKind.DOC_UNWIND );

    MqlFunctionOperator DOC_EXISTS = new MqlFunctionOperator( "DOC_EXISTS", SqlKind.DOC_EXISTS );

    MqlFunctionOperator DOC_LT = new MqlFunctionOperator( "DOC_LT", SqlKind.LESS_THAN );

    MqlFunctionOperator DOC_GT = new MqlFunctionOperator( "DOC_GT", SqlKind.GREATER_THAN );

    MqlFunctionOperator DOC_LTE = new MqlFunctionOperator( "DOC_LTE", SqlKind.LESS_THAN_OR_EQUAL );

    MqlFunctionOperator DOC_GTE = new MqlFunctionOperator( "DOC_GTE", SqlKind.GREATER_THAN_OR_EQUAL );

    MqlFunctionOperator DOC_JSONIZE = new MqlFunctionOperator( "DOC_JSONIZE", SqlKind.UPDATE );
}
