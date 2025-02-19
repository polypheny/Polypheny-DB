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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.core.common.Modify.Operation;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactoryImpl;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.processing.DeepCopyShuttle;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.transaction.Statement;
import org.polypheny.db.transaction.mvcc.RelCommitStateFilterRewrite.CommitState;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeFactoryImpl;

public class RewriteUpdatingRelModify implements AlgTreeModification<LogicalRelModify, LogicalRelModify> {

    private final Statement statement;


    public RewriteUpdatingRelModify( Statement statement ) {
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

        AlgRoot updatingSubtree = createUpdatingSubtree( node );
        MvccUtils.executeDmlAlgTree( updatingSubtree, statement, node.getEntity().getNamespaceId() );
        return createInsertingSubtree( node );
    }


    private LogicalRelModify createInsertingSubtree( LogicalRelModify originalModify ) {
        throwOnIllegalInputNode( originalModify );
        /*
        Idea: We know that if we move down the tree there will be a join at some point due to the mvcc snapshot filtering.
        There we will place the filter into the left subtree to remove versions accordingly.
         */
        MvccJoinLhsFilterRewriter lhsFilterRewriter = new MvccJoinLhsFilterRewriter( CommitState.COMMITTED );
        LogicalRelProject originalProject = (LogicalRelProject) lhsFilterRewriter.visit( (LogicalRelProject) originalModify.getInput() );

        List<AlgDataTypeField> inputFields = originalProject.getRowType().getFields().stream()
                .filter( f -> !(originalProject.getProjects().get( f.getIndex() ) instanceof RexLiteral) ).collect( Collectors.toCollection( ArrayList::new ) );


        List<RexNode> projects = new ArrayList<>( inputFields.size() );

        for ( int i = 0; i < inputFields.size(); i++ ) {
            AlgDataTypeField field = inputFields.get( i );
            if ( i == 1 ) {
                // replace _vid
                projects.add( new RexLiteral(
                        IdentifierUtils.getVersionAsPolyBigDecimal( statement.getTransaction().getSequenceNumber(), false ),
                        IdentifierUtils.VERSION_ALG_TYPE,
                        PolyType.BIGINT
                ) );
                continue;
            }

            if ( originalModify.getUpdateColumns().contains( field.getName() ) ) {
                // replace updated values
                int updateIndex = originalModify.getUpdateColumns().indexOf( field.getName() );
                projects.add( originalModify.getSourceExpressions().get( updateIndex ) );
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


    private AlgRoot createUpdatingSubtree( LogicalRelModify originalModify ) {
        originalModify = (LogicalRelModify) new DeepCopyShuttle().visit( originalModify );
        throwOnIllegalInputNode( originalModify );

        /*
        Idea: We know that if we move down the tree there will be a join at some point due to the mvcc snapshot filtering.
        There we will place the filter into the left subtree to remove versions accordingly.
         */
        MvccJoinLhsFilterRewriter lhsFilterRewriter = new MvccJoinLhsFilterRewriter( CommitState.UNCOMMITTED );
        LogicalRelProject originalProject = (LogicalRelProject) lhsFilterRewriter.visit( originalModify.getInput() );

        List<AlgDataTypeField> inputFields = originalProject.getRowType().getFields().stream()
                .filter( f -> !(originalProject.getProjects().get( f.getIndex() ) instanceof RexLiteral) ).collect( Collectors.toCollection( ArrayList::new ) );

        List<RexNode> projects = new ArrayList<>( inputFields.size() );

        for ( int i = 0; i < inputFields.size(); i++ ) {
            AlgDataTypeField field = inputFields.get( i );
            if ( i == 1 ) {
                // replace _vid
                projects.add( new RexLiteral(
                        IdentifierUtils.getVersionAsPolyBigDecimal( statement.getTransaction().getSequenceNumber(), false ),
                        IdentifierUtils.VERSION_ALG_TYPE,
                        PolyType.BIGINT
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

        LogicalRelModify updatingModify = LogicalRelModify.create(
                originalModify.getEntity(),
                project,
                Operation.UPDATE,
                originalModify.getUpdateColumns(),
                originalModify.getSourceExpressions(),
                false
        );

        return AlgRoot.of( updatingModify, Kind.UPDATE );


    }

}
