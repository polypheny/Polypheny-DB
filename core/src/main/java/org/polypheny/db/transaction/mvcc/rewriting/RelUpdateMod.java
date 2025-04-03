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
import java.util.stream.Collectors;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.common.Modify.Operation;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.processing.DeepCopyShuttle;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.mvcc.IdentifierUtils;
import org.polypheny.db.transaction.mvcc.MvccUtils;

public class RelUpdateMod implements AlgTreeModification<LogicalRelModify, LogicalRelModify> {

    private final Statement statement;


    public RelUpdateMod( Statement statement ) {
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

        List<AlgDataTypeField> inputFields = originalProject.getRowType().getFields().stream()
                .filter( f -> !(originalProject.getProjects().get( f.getIndex() ) instanceof RexLiteral) ).collect( Collectors.toCollection( ArrayList::new ) );

        assert IdentifierUtils.IDENTIFIER_KEY.equals( inputFields.get( 0 ).getName() );
        assert IdentifierUtils.VERSION_KEY.equals( inputFields.get( 1 ).getName() );

        LogicalRelModify updatingSubtree = createUpdatingSubtree( node );

        //TODO TH: remove debug
        long startTs = System.nanoTime();

        MvccUtils.executeDmlAlgTree( AlgRoot.of( updatingSubtree, Kind.UPDATE ), statement, node.getEntity().getNamespaceId() );

        long endTs = System.nanoTime();
        System.out.printf( "UUU %d", endTs - startTs );

        return createInsertingSubtree( node );
    }


    private LogicalRelModify createInsertingSubtree( LogicalRelModify originalModify ) {
        throwOnIllegalInputNode( originalModify );
        /*
        Idea: We know that if we move down the tree there will be a join at some point due to the mvcc snapshot filtering.
        There we will place the filter into the left subtree to remove versions accordingly.
         */
        RelSnapshotFilterRewriter filterRewriter = new RelSnapshotFilterRewriter( CommitState.COMMITTED, originalModify.getEntity() );
        LogicalRelProject originalProject = (LogicalRelProject) filterRewriter.visit( (LogicalRelProject) originalModify.getInput() );

        List<RexNode> originalProjects = originalProject.getProjects().stream()
                .filter( p -> p instanceof RexIndexRef )
                .toList();

        List<RexNode> newProjects = new ArrayList<>( originalProjects.size() );
        List<String> newFieldNames = new ArrayList<>( originalProjects.size() );
        for ( int i = 0; i < originalProjects.size(); i++ ) {
            AlgDataTypeField originalField = originalProject.getRowType().getFields().get( i );
            newFieldNames.add( originalField.getName() );
            if ( i == 1 ) {
                // replace _vid
                newProjects.add( new RexLiteral(
                        IdentifierUtils.getVersionAsPolyBigDecimal( statement.getTransaction().getSequenceNumber(), false ),
                        IdentifierUtils.VERSION_ALG_TYPE,
                        IdentifierUtils.VERSION_ALG_TYPE.getPolyType()
                ) );
                continue;
            }

            if ( originalModify.getUpdateColumns().contains( originalField.getName() ) ) {
                // replace updated values
                int updateIndex = originalModify.getUpdateColumns().indexOf( originalField.getName() );
                RexNode sourceExp = originalModify.getSourceExpressions().get( updateIndex );
                if ( sourceExp instanceof RexCall ) {
                    newProjects.add( originalProject.getProjects().get( originalProjects.size() + updateIndex ) );
                    continue;
                }
                newProjects.add( sourceExp );
                continue;
            }

            newProjects.add( originalProjects.get( i ) );
        }

        LogicalRelProject project = LogicalRelProject.create(
                originalProject.getInput(),
                newProjects,
                newFieldNames
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


    private LogicalRelModify createUpdatingSubtree( LogicalRelModify originalModify ) {
        originalModify = (LogicalRelModify) new DeepCopyShuttle().visit( originalModify );
        throwOnIllegalInputNode( originalModify );

        /*
        Idea: We know that if we move down the tree there will be a join at some point due to the mvcc snapshot filtering.
        There we will place the filter into the left subtree to remove versions accordingly.
         */
        RelSnapshotFilterRewriter filterRewriter = new RelSnapshotFilterRewriter( CommitState.UNCOMMITTED, originalModify.getEntity() );
        LogicalRelProject originalProject = (LogicalRelProject) filterRewriter.visit( originalModify.getInput() );

        return LogicalRelModify.create(
                originalModify.getEntity(),
                originalProject,
                Operation.UPDATE,
                originalModify.getUpdateColumns(),
                originalModify.getSourceExpressions(),
                false
        );


    }

}
