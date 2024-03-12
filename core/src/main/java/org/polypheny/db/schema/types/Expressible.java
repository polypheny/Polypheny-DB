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

package org.polypheny.db.schema.types;

import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;

public interface Expressible {

    /**
     * Returns the value as an {@link Expression}. Which can be used for code generation.
     *
     * @return the value as an {@link Expression}
     */
    Expression asExpression();

    /**
     * Returns the value as an {@link Expression} and forces a specific clas. Which can be used for code generation.
     *
     * @param clazz the class to force
     * @return the value as an {@link Expression}
     */
    default Expression asExpression( Class<?> clazz ) {
        return Expressions.convert_( asExpression(), clazz );
    }

}
