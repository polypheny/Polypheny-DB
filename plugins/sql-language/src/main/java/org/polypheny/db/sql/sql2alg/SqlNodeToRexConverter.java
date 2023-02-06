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

package org.polypheny.db.sql.sql2alg;


import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.sql.language.SqlCall;
import org.polypheny.db.sql.language.SqlIntervalQualifier;
import org.polypheny.db.sql.language.SqlLiteral;
import org.polypheny.db.sql.language.SqlNode;


/**
 * Converts expressions from {@link SqlNode} to {@link RexNode}.
 */
public interface SqlNodeToRexConverter {

    /**
     * Converts a {@link SqlCall} to a {@link RexNode} expression.
     */
    RexNode convertCall( SqlRexContext cx, SqlCall call );

    /**
     * Converts a {@link SqlLiteral SQL literal} to a {@link RexLiteral REX literal}.
     *
     * The result is {@link RexNode}, not {@link RexLiteral} because if the literal is NULL (or the boolean Unknown value), we make a <code>CAST(NULL AS type)</code> expression.
     */
    RexNode convertLiteral( SqlRexContext cx, SqlLiteral literal );

    /**
     * Converts a {@link SqlIntervalQualifier SQL Interval Qualifier} to a {@link RexLiteral REX literal}.
     */
    RexLiteral convertInterval( SqlRexContext cx, SqlIntervalQualifier intervalQualifier );

}

