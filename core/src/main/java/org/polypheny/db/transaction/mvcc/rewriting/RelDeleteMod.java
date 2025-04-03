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

package org.polypheny.db.transaction.mvcc.rewriting;

import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.common.Modify.Operation;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.mvcc.IdentifierUtils;
import org.polypheny.db.transaction.mvcc.MvccUtils;

public class RelDeleteMod implements AlgTreeModification<LogicalRelModify, LogicalRelModify> {

    private final Statement statement;


    public RelDeleteMod( Statement statement ) {
        this.statement = statement;
    }


    private void throwOnIllegalInputNode( LogicalRelModify modify ) {
        if ( !(modify.getInput() instanceof LogicalRelProject) ) {
            throw new IllegalStateException( "Project expected as input to updating rel modify" );
        }
    }


    @Override
    public LogicalRelModify apply( LogicalRelModify node ) {
        throwOnIllegalInputNode( node );
        LogicalRelProject originalProject = (LogicalRelProject) node.getInput();

        List<AlgDataTypeField> inputFields = originalProject.getRowType().getFields();

        assert IdentifierUtils.IDENTIFIER_KEY.equals( inputFields.get( 0 ).getName() );
        assert IdentifierUtils.VERSION_KEY.equals( inputFields.get( 1 ).getName() );

        AlgRoot deletingSubtree = createDeletingSubtree( node );

        long startTs = System.nanoTime();

        MvccUtils.executeDmlAlgTree( deletingSubtree, statement, node.getEntity().getNamespaceId() );

        long endTs = System.nanoTime();
        System.out.printf( "DDD %d\n", endTs - startTs );

        return createInsertingSubtree( node );
    }


    private AlgRoot createDeletingSubtree( LogicalRelModify originalModify ) {
        throwOnIllegalInputNode( originalModify );
        /*
        Idea: We know that if we move down the tree there will be a join at some point due to the mvcc snapshot filtering.
        There we will place the filter into the left subtree to remove versions accordingly.
         */
        RelSnapshotFilterRewriter filterRewriter = new RelSnapshotFilterRewriter( CommitState.UNCOMMITTED, originalModify.getEntity() );
        LogicalRelProject originalProject = (LogicalRelProject) filterRewriter.visit( (LogicalRelProject) originalModify.getInput() );

        LogicalRelModify deletingModify = LogicalRelModify.create(
                originalModify.getEntity(),
                originalProject,
                Operation.DELETE,
                null,
                null,
                false
        );

        return AlgRoot.of( deletingModify, Kind.DELETE );
    }


    private LogicalRelModify createInsertingSubtree( LogicalRelModify originalModify ) {
        throwOnIllegalInputNode( originalModify );
        /*
        Idea: We know that if we move down the tree there will be a join at some point due to the mvcc snapshot filtering.
        There we will place the filter into the left subtree to remove versions accordingly.
         */
        RelSnapshotFilterRewriter filterRewriter = new RelSnapshotFilterRewriter( CommitState.COMMITTED, originalModify.getEntity() );
        LogicalRelProject originalProject = (LogicalRelProject) filterRewriter.visit( (LogicalRelProject) originalModify.getInput() );

        List<AlgDataTypeField> inputFields = originalProject.getRowType().getFields();

        List<RexNode> projects = new ArrayList<>( inputFields.size() );

        for ( int i = 0; i < inputFields.size(); i++ ) {
            AlgDataTypeField field = inputFields.get( i );
            if ( i == 0 ) {
                // swap the sign of the entity id
                projects.add( new RexCall(
                        IdentifierUtils.IDENTIFIER_ALG_TYPE,
                        OperatorRegistry.get( OperatorName.UNARY_MINUS ),
                        new RexIndexRef( field.getIndex(), field.getType() ) ) );
                continue;
            }

            if ( i == 1 ) {
                // replace _vid
                projects.add( new RexLiteral(
                        IdentifierUtils.getVersionAsPolyBigDecimal( statement.getTransaction().getSequenceNumber(), false ),
                        IdentifierUtils.VERSION_ALG_TYPE,
                        IdentifierUtils.VERSION_ALG_TYPE.getPolyType()
                ) );
                continue;
            }

            projects.add( new RexIndexRef( field.getIndex(), field.getType() ) );
        }

        LogicalRelProject project = LogicalRelProject.create(
                originalProject.getInput(),
                projects,
                inputFields.stream().map( AlgDataTypeField::getName ).toList()
        );

        return LogicalRelModify.create(
                originalModify.getEntity(),
                project,
                Operation.INSERT,
                null,
                null,
                false
        );
    }

}
