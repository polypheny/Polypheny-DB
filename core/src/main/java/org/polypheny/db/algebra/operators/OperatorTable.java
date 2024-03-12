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

package org.polypheny.db.algebra.operators;


import java.util.List;
import org.polypheny.db.algebra.constant.FunctionCategory;
import org.polypheny.db.algebra.constant.Syntax;
import org.polypheny.db.nodes.Identifier;
import org.polypheny.db.nodes.Operator;


/**
 * SqlOperatorTable defines a directory interface for enumerating and looking up operators and functions.
 */
public interface OperatorTable {

    /**
     * Retrieves a list of operators with a given name and syntax. For example, by passing SqlSyntax.Function, the returned list is narrowed to only matching SqlFunction objects.
     *
     * @param opName name of operator
     * @param category function category to look up, or null for any matching operator
     * @param syntax syntax type of operator
     * @param operatorList mutable list to which to append matches
     */
    void lookupOperatorOverloads( Identifier opName, FunctionCategory category, Syntax syntax, List<Operator> operatorList );

    /**
     * Retrieves a list of all functions and operators in this table. Used for automated testing.
     *
     * @return list of SqlOperator objects
     */
    List<? extends Operator> getOperatorList();

}

