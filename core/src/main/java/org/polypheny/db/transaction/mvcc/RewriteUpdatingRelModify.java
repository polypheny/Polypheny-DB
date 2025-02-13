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
import org.polypheny.db.algebra.core.common.Modify.Operation;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;

public class RewriteUpdatingRelModify implements AlgTreeModification<LogicalRelModify, LogicalRelModify> {

    private final long sequenceNumber;


    public RewriteUpdatingRelModify( long sequenceNumber ) {
        this.sequenceNumber = sequenceNumber;
    }


    @Override
    public LogicalRelModify apply( LogicalRelModify node ) {
        if ( !(node.getInput() instanceof LogicalRelProject originalProject) ) {
            throw new IllegalStateException( "Project expected as input to updating rel modify" );
        }

        List<AlgDataTypeField> inputFields = originalProject.getRowType().getFields().stream()
                .filter( f -> !(originalProject.getProjects().get( f.getIndex() ) instanceof RexLiteral) ).collect( Collectors.toCollection( ArrayList::new ) );

        assert IdentifierUtils.IDENTIFIER_KEY.equals( inputFields.get( 0 ).getName() );
        assert IdentifierUtils.VERSION_KEY.equals( inputFields.get( 1 ).getName() );

        List<RexNode> projects = new ArrayList<>( inputFields.size() );

        for ( int i = 0; i < inputFields.size(); i++ ) {
            AlgDataTypeField field = inputFields.get( i );
            if ( i == 1 ) {
                // replace _vid
                projects.add( new RexLiteral(
                        IdentifierUtils.getVersionAsPolyBigDecimal( sequenceNumber, false ),
                        IdentifierUtils.VERSION_ALG_TYPE,
                        PolyType.BIGINT
                ) );
                continue;
            }

            if ( node.getUpdateColumns().contains( field.getName() ) ) {
                // replace updated values
                int updateIndex = node.getUpdateColumns().indexOf( field.getName() );
                projects.add( node.getSourceExpressions().get( updateIndex ) );
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
                node.getEntity(),
                project,
                Operation.INSERT,
                null,
                null,
                false
        );
    }

}
