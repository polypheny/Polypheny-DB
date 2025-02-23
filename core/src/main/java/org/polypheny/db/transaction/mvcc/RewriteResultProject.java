/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.transaction.mvcc;

import java.util.List;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexVisitor;
import org.polypheny.db.rex.RexVisitorImpl;
import org.polypheny.db.util.Util;

public class RewriteResultProject implements AlgTreeModification<LogicalRelProject, LogicalRelProject> {

    @Override
    public LogicalRelProject apply( LogicalRelProject node ) {
        List<String> oldFieldNames = node.getRowType().getFieldNames();
        List<RexNode> rexNodes = node.getProjects().stream()
                .filter( n -> !isRefToMvccField( n, oldFieldNames ) )
                .toList();
        List<String> newFieldNames = node.getRowType().getFieldNames().stream()
                .filter( n -> !n.equals( IdentifierUtils.IDENTIFIER_KEY ) && !n.equals( IdentifierUtils.VERSION_KEY ) )
                .toList();
        return LogicalRelProject.create(
                node.getInput(),
                rexNodes,
                newFieldNames
        );
    }


    public boolean isRefToMvccField( RexNode node, List<String> fieldNames ) {
        try {
            RexVisitor<Void> visitor =
                    new RexVisitorImpl<>( true ) {
                        @Override
                        public Void visitIndexRef( RexIndexRef inputRef ) {
                            if ( fieldNames.get( inputRef.getIndex() ).equals( IdentifierUtils.IDENTIFIER_KEY ) ) {
                                throw new Util.FoundOne( inputRef );
                            }
                            if ( fieldNames.get( inputRef.getIndex() ).equals( IdentifierUtils.VERSION_KEY ) ) {
                                throw new Util.FoundOne( inputRef );
                            }
                            return null;
                        }
                    };
            node.accept( visitor );
            return false;
        } catch ( Util.FoundOne e ) {
            Util.swallow( e, null );
            return true;
        }
    }

}
