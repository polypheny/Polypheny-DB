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

package org.polypheny.db.nodes;

import java.util.List;
import java.util.Set;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.util.Litmus;

public interface Node extends Cloneable, Visitable {


    /**
     * Returns whether two nodes are equal (using {@link #equalsDeep(Node, Litmus)}) or are both null.
     *
     * @param node1 First expression
     * @param node2 Second expression
     * @param litmus What to do if an error is detected (expressions are not equal)
     */
    static boolean equalDeep( Node node1, Node node2, Litmus litmus ) {
        if ( node1 == null ) {
            return node2 == null;
        } else if ( node2 == null ) {
            return false;
        } else {
            return node1.equalsDeep( node2, litmus );
        }
    }

    /**
     * Returns whether two lists of operands are equal.
     */
    static boolean equalDeep( List<? extends Node> operands0, List<? extends Node> operands1, Litmus litmus ) {
        if ( operands0.size() != operands1.size() ) {
            return litmus.fail( null );
        }
        for ( int i = 0; i < operands0.size(); i++ ) {
            if ( !Node.equalDeep( operands0.get( i ), operands1.get( i ), litmus ) ) {
                return litmus.fail( null );
            }
        }
        return litmus.succeed();
    }

    /**
     * Creates a copy of a SqlNode.
     */
    static <E extends Node> E clone( E e ) {
        //noinspection unchecked
        return (E) e.clone( e.getPos() );
    }

    /**
     * Clones a SqlNode with a different position.
     */
    Node clone( ParserPos pos );

    Kind getKind();

    QueryLanguage getLanguage();

    boolean isA( Set<Kind> category );

    String toString();

    ParserPos getPos();

    default boolean isDdl() {
        return Kind.DDL.contains( getKind() );
    }

    default long getNamespaceId() {
        return Catalog.defaultNamespaceId;
    }

    @Nullable
    default String getNamespaceName() {
        return null;
    }

    /**
     * Returns whether this node is structurally equivalent to another node.
     * Some examples:
     *
     * <ul>
     * <li>1 + 2 is structurally equivalent to 1 + 2</li>
     * <li>1 + 2 + 3 is structurally equivalent to (1 + 2) + 3, but not to 1 + (2 + 3), because the '+' operator is left-associative</li>
     * </ul>
     */
    boolean equalsDeep( Node node, Litmus litmus );

    @Nullable String getEntity();

}
