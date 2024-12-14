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

package org.polypheny.db.transaction.locking;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.core.common.Modify.Operation;
import org.polypheny.db.algebra.logical.common.LogicalConditionalExecute;
import org.polypheny.db.algebra.logical.common.LogicalConstraintEnforcer;
import org.polypheny.db.algebra.logical.document.LogicalDocumentAggregate;
import org.polypheny.db.algebra.logical.document.LogicalDocumentFilter;
import org.polypheny.db.algebra.logical.document.LogicalDocumentModify;
import org.polypheny.db.algebra.logical.document.LogicalDocumentProject;
import org.polypheny.db.algebra.logical.document.LogicalDocumentScan;
import org.polypheny.db.algebra.logical.document.LogicalDocumentSort;
import org.polypheny.db.algebra.logical.document.LogicalDocumentTransformer;
import org.polypheny.db.algebra.logical.document.LogicalDocumentValues;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgAggregate;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgFilter;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgMatch;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgModify;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgProject;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgScan;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgSort;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgTransformer;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgUnwind;
import org.polypheny.db.algebra.logical.lpg.LogicalLpgValues;
import org.polypheny.db.algebra.logical.relational.LogicalRelAggregate;
import org.polypheny.db.algebra.logical.relational.LogicalRelCorrelate;
import org.polypheny.db.algebra.logical.relational.LogicalRelExchange;
import org.polypheny.db.algebra.logical.relational.LogicalRelFilter;
import org.polypheny.db.algebra.logical.relational.LogicalRelIntersect;
import org.polypheny.db.algebra.logical.relational.LogicalRelJoin;
import org.polypheny.db.algebra.logical.relational.LogicalRelMatch;
import org.polypheny.db.algebra.logical.relational.LogicalRelMinus;
import org.polypheny.db.algebra.logical.relational.LogicalRelModify;
import org.polypheny.db.algebra.logical.relational.LogicalRelProject;
import org.polypheny.db.algebra.logical.relational.LogicalRelScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelSort;
import org.polypheny.db.algebra.logical.relational.LogicalRelTableFunctionScan;
import org.polypheny.db.algebra.logical.relational.LogicalRelUnion;
import org.polypheny.db.algebra.logical.relational.LogicalRelValues;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactoryImpl;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.AlgDataTypeFieldImpl;
import org.polypheny.db.algebra.type.AlgRecordType;
import org.polypheny.db.algebra.type.StructKind;
import org.polypheny.db.plan.AlgOptUtil;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexUtil;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyLong;

public class AlgTreeRewriter extends AlgShuttleImpl {


    public static AlgRoot process( AlgRoot root ) {
        return root.withAlg( root.alg.accept( new AlgTreeRewriter() ) );
    }


    @Override
    public AlgNode visit( LogicalRelAggregate aggregate ) {
        return visitChild( aggregate, 0, aggregate.getInput() );
    }


    @Override
    public AlgNode visit( LogicalRelMatch match ) {
        return visitChild( match, 0, match.getInput() );
    }


    @Override
    public AlgNode visit( LogicalRelScan scan ) {
        return scan;
    }


    @Override
    public AlgNode visit( LogicalRelTableFunctionScan scan ) {
        return visitChildren( scan );
    }


    @Override
    public AlgNode visit( LogicalRelValues values ) {
        /*
            Inserts can be divided into two categories according to the nature of their input:
            1) Values as input
            2) Inputs that are not values.

            In case 1, we already add the identifiers here by modifying the values.
            Case 2 is handled using rules in the planner.
         */
        ImmutableList<ImmutableList<RexLiteral>> newValues = values.tuples.stream()
                .map(row -> ImmutableList.<RexLiteral>builder()
                        .add(IdentifierUtils.getIdentifierAsLiteral())
                        .addAll(row)
                        .build())
                .collect(ImmutableList.toImmutableList());

        List<AlgDataTypeField> newFields = new ArrayList<>();
        newFields.add(new AlgDataTypeFieldImpl(0L, "_eid", "_eid", 0, IdentifierUtils.IDENTIFIER_ALG_TYPE ));
        values.getRowType().getFields().stream()
                .map(f -> new AlgDataTypeFieldImpl(f.getId(), f.getName(), f.getPhysicalName(), f.getIndex() + 1, f.getType()))
                .forEach(newFields::add);

        AlgDataType newRowType = new AlgRecordType( StructKind.FULLY_QUALIFIED, newFields );

        return LogicalRelValues.create( values.getCluster(), newRowType, newValues );
    }

    @Override
    public AlgNode visit( LogicalRelFilter filter ) {
        return visitChild( filter, 0, filter.getInput() );
    }


    @Override
    public AlgNode visit( LogicalRelProject project ) {
        AlgNode input = project.getInput();
        if (! (input instanceof LogicalRelValues oldValues)) {
            return project;
        }
         /*
            When values are used for an insert, they are modified.
            The project has to be adjusted accordingly.
         */
        LogicalRelValues newValues = (LogicalRelValues) visit(oldValues);
        List<RexNode> newProjects = createNewProjects(project, newValues);
        return project.copy(project.getTraitSet(), newValues, newProjects, project.getRowType());
    }

    private List<RexNode> createNewProjects(LogicalRelProject project, LogicalRelValues inputValues) {
        List<RexNode> newProjects = new LinkedList<>();
        long fieldCount = project.getRowType().getFieldCount();
        long inputFieldCount = inputValues.tuples.get(0).size();

        for (int position = 0; position < fieldCount; position++) {
            if (position < inputFieldCount) {
                newProjects.add(new RexIndexRef(
                        position,
                        project.getRowType().getFields().get(position).getType()
                ));
            } else {
                newProjects.add(new RexLiteral(
                        null,
                        project.getRowType().getFields().get(position).getType(),
                        project.getRowType().getFields().get(position).getType().getPolyType()
                ));
            }
        }

        return newProjects;
    }


    @Override
    public AlgNode visit( LogicalRelJoin join ) {
        return visitChildren( join );
    }


    @Override
    public AlgNode visit( LogicalRelCorrelate correlate ) {
        return visitChildren( correlate );
    }


    @Override
    public AlgNode visit( LogicalRelUnion union ) {
        return visitChildren( union );
    }


    @Override
    public AlgNode visit( LogicalRelIntersect intersect ) {
        return visitChildren( intersect );
    }


    @Override
    public AlgNode visit( LogicalRelMinus minus ) {
        return visitChildren( minus );
    }


    @Override
    public AlgNode visit( LogicalRelSort sort ) {
        return visitChildren( sort );
    }


    @Override
    public AlgNode visit( LogicalRelExchange exchange ) {
        return visitChildren( exchange );
    }


    @Override
    public AlgNode visit( LogicalConditionalExecute lce ) {
        return visitChildren( lce );
    }


    @Override
    public AlgNode visit( LogicalRelModify modify ) {
            return visitChildren( modify );
    }


    private List<RexNode> getProjects( AlgDataType rowType ) {
        return rowType.getFields().stream()
                .filter( f -> !f.getName().equals( IdentifierUtils.IDENTIFIER_KEY ) )
                .map( f -> new RexIndexRef( f.getIndex(), f.getType() ) )
                .collect( Collectors.toCollection( LinkedList::new ) );
    }


    @Override
    public AlgNode visit( LogicalConstraintEnforcer enforcer ) {
        return visitChildren( enforcer );
    }


    @Override
    public AlgNode visit( LogicalLpgModify modify ) {
        return visitChildren( modify );
    }


    @Override
    public AlgNode visit( LogicalLpgScan scan ) {
        return scan;
    }


    @Override
    public AlgNode visit( LogicalLpgValues values ) {
        return values;
    }


    @Override
    public AlgNode visit( LogicalLpgFilter filter ) {
        return visitChildren( filter );
    }


    @Override
    public AlgNode visit( LogicalLpgMatch match ) {
        return visitChildren( match );
    }


    @Override
    public AlgNode visit( LogicalLpgProject project ) {
        return visitChildren( project );
    }


    @Override
    public AlgNode visit( LogicalLpgAggregate aggregate ) {
        return visitChildren( aggregate );
    }


    @Override
    public AlgNode visit( LogicalLpgSort sort ) {
        return visitChildren( sort );
    }


    @Override
    public AlgNode visit( LogicalLpgUnwind unwind ) {
        return visitChildren( unwind );
    }


    @Override
    public AlgNode visit( LogicalLpgTransformer transformer ) {
        return visitChildren( transformer );
    }


    @Override
    public AlgNode visit( LogicalDocumentModify modify ) {
        return visitChildren( modify );
    }


    @Override
    public AlgNode visit( LogicalDocumentAggregate aggregate ) {
        return visitChildren( aggregate );
    }


    @Override
    public AlgNode visit( LogicalDocumentFilter filter ) {
        return visitChildren( filter );
    }


    @Override
    public AlgNode visit( LogicalDocumentProject project ) {
        return visitChildren( project );
    }


    @Override
    public AlgNode visit( LogicalDocumentScan scan ) {
        return visitChildren( scan );
    }


    @Override
    public AlgNode visit( LogicalDocumentSort sort ) {
        return visitChildren( sort );
    }


    @Override
    public AlgNode visit( LogicalDocumentTransformer transformer ) {
        return visitChildren( transformer );
    }


    @Override
    public AlgNode visit( LogicalDocumentValues values ) {
        return visitChildren( values );
    }


    @Override
    public AlgNode visit( AlgNode other ) {
        return visitChildren( other );

    }

}