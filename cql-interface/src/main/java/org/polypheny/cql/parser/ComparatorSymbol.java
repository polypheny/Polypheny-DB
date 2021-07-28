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

package org.polypheny.cql.parser;

import org.polypheny.db.sql.SqlBinaryOperator;
import org.polypheny.db.sql.fun.SqlStdOperatorTable;

public enum ComparatorSymbol {
    SERVER_CHOICE,
    EQUALS,
    NOT_EQUALS,
    GREATER_THAN,
    LESS_THAN,
    GREATER_THAN_OR_EQUALS,
    LESS_THAN_OR_EQUALS;


    public SqlBinaryOperator toSqlStdOperatorTable( SqlBinaryOperator fallback ) {
        if ( this == SERVER_CHOICE ) {
            return fallback;
        } else if ( this == EQUALS ) {
            return SqlStdOperatorTable.EQUALS;
        } else if ( this == NOT_EQUALS ) {
            return SqlStdOperatorTable.NOT_EQUALS;
        } else if ( this == GREATER_THAN ) {
            return SqlStdOperatorTable.GREATER_THAN;
        } else if ( this == LESS_THAN ) {
            return SqlStdOperatorTable.LESS_THAN;
        } else if ( this == GREATER_THAN_OR_EQUALS ) {
            return SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
        } else {
            return SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
        }
    }
}
