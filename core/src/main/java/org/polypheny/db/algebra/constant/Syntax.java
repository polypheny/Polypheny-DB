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

package org.polypheny.db.algebra.constant;

import org.polypheny.db.util.Conformance;

public enum Syntax {
    /**
     * Function syntax, as in "Foo(x, y)".
     */
    FUNCTION,

    /**
     * Function syntax, as in "Foo(x, y)", but uses "*" if there are no arguments, for example "COUNT(*)".
     */
    FUNCTION_STAR,

    /**
     * Binary operator syntax, as in "x + y".
     */
    BINARY,

    /**
     * Prefix unary operator syntax, as in "- x".
     */
    PREFIX,

    /**
     * Postfix unary operator syntax, as in "x ++".
     */
    POSTFIX,

    /**
     * Special syntax, such as that of the SQL CASE operator, "CASE x WHEN 1 THEN 2 ELSE 3 END".
     */
    SPECIAL,

    /**
     * Function syntax which takes no parentheses if there are no arguments, for example "CURRENTTIME".
     *
     * @see Conformance#allowNiladicParentheses()
     */
    FUNCTION_ID,

    /**
     * Syntax of an internal operator, which does not appear in the SQL.
     */
    INTERNAL;
}
