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

package org.polypheny.db.nodes;


/**
 * Visitor class, follows the {@link org.polypheny.db.util.Glossary#VISITOR_PATTERN visitor pattern}.
 *
 * The type parameter <code>R</code> is the return type of each <code>visit()</code> method. If the methods do not need to return a value, use {@link Void}.
 *
 * @param <R> Return type
 * #@see SqlBasicVisitor
 * @see Node#accept(NodeVisitor)
 * #@see Operator#acceptCall
 */
public interface NodeVisitor<R> {

    /**
     * Visits a literal.
     *
     * @param literal Literal
     * @see Literal#accept(NodeVisitor)
     */
    R visit( Literal literal );

    /**
     * Visits a call to a {@link OperatorImpl}.
     *
     * @param call Call
     * @see Call#accept(NodeVisitor)
     */
    R visit( Call call );

    /**
     * Visits a list of {@link Node} objects.
     *
     * @param nodeList list of nodes
     * @see NodeList#accept(NodeVisitor)
     */
    R visit( NodeList nodeList );

    /**
     * Visits an identifier.
     *
     * @param id identifier
     * @see Identifier#accept(NodeVisitor)
     */
    R visit( Identifier id );

    /**
     * Visits a datatype specification.
     *
     * @param type datatype specification
     * @see DataTypeSpec#accept(NodeVisitor)
     */
    R visit( DataTypeSpec type );

    /**
     * Visits a dynamic parameter.
     *
     * @param param Dynamic parameter
     * @see DynamicParam#accept(NodeVisitor)
     */
    R visit( DynamicParam param );

    /**
     * Visits an interval qualifier
     *
     * @param intervalQualifier Interval qualifier
     * @see IntervalQualifier#accept(NodeVisitor)
     */
    R visit( IntervalQualifier intervalQualifier );

}
