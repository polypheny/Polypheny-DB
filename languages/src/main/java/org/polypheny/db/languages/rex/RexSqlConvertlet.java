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

package org.polypheny.db.languages.rex;


import org.polypheny.db.core.Node;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexNode;


/**
 * Converts a {@link RexNode} expression into a {@link Node} expression.
 */
public interface RexSqlConvertlet {

    /**
     * Converts a {@link RexCall} to a {@link Node} expression.
     *
     * @param converter to use in translating
     * @param call RexCall to translate
     * @return SqlNode, or null if translation was unavailable
     */
    Node convertCall( RexToSqlNodeConverter converter, RexCall call );

}

