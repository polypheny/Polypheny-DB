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

package org.polypheny.db.algebra.enumerable;


import java.util.List;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.rex.RexNode;


/**
 * Information for a call to {@link AggImplementor#implementAdd(AggContext, AggAddContext)}.
 *
 * Typically, the aggregation implementation will use {@link #arguments()} or {@link #rexArguments()} to update aggregate value.
 */
public interface AggAddContext extends AggResultContext {

    /**
     * Returns {@link RexNode} representation of arguments. This can be useful for manual translation of required arguments with different {@link NullPolicy}.
     *
     * @return {@link RexNode} representation of arguments
     */
    List<RexNode> rexArguments();

    /**
     * Returns {@link RexNode} representation of the filter, or null.
     */
    RexNode rexFilterArgument();

    /**
     * Returns Linq4j form of arguments. The resulting value is equivalent to {@code rowTranslator().translateList(rexArguments())}. This is handy if you need just operate on argument.
     *
     * @return Linq4j form of arguments.
     */
    List<Expression> arguments();

    /**
     * Returns a {@link RexToLixTranslator} suitable to transform the arguments.
     *
     * @return {@link RexToLixTranslator} suitable to transform the arguments
     */
    RexToLixTranslator rowTranslator();
}

