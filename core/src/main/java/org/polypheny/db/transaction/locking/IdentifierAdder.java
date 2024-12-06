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
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgRoot;
import org.polypheny.db.algebra.AlgShuttleImpl;
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
import org.polypheny.db.algebra.type.AlgDataTypeFactoryImpl;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.numerical.PolyLong;

public class IdentifierAdder extends AlgShuttleImpl {

    private static final PolyLong missingIdentifier = new PolyLong( IdentifierUtils.MISSING_IDENTIFIER );


    public static AlgRoot process( AlgRoot root ) {
        return root.withAlg( root.alg.accept( new IdentifierAdder() ) );
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
        ImmutableList<ImmutableList<RexLiteral>> immutableValues = values.tuples.stream()
                .map( row -> {
                    RexLiteral identifierLiteral = row.get( 0 );
                    if ( !identifierLiteral.getValue().equals( missingIdentifier ) ) {
                        /*
                        This method is only called on the top level of an insert.
                        The values present are thus all inserted by the user.
                        If identifiers are present at this stage, they were added by the user.
                         */
                        IdentifierUtils.throwIllegalFieldName();
                    }
                    PolyValue identifier = IdentifierUtils.getIdentifier();
                    return ImmutableList.<RexLiteral>builder()
                            .add( new RexLiteral( identifier, identifierLiteral.getType(), PolyType.DECIMAL ) )
                            .addAll( row.subList( 1, row.size() ) )
                            .build();

                } )
                .collect( ImmutableList.toImmutableList() );

        return LogicalRelValues.create( values.getCluster(), values.getRowType(), immutableValues );
    }


    @Override
    public AlgNode visit( LogicalRelFilter filter ) {
        return visitChild( filter, 0, filter.getInput() );
    }


    @Override
    public AlgNode visit(LogicalRelProject project) {
        // Detect underpopulated columns if the user specifies fewer values than there are columns in the table
        Optional<Integer> indexOpt = findIdentifierIndex(project);
        if (indexOpt.isEmpty()) {
            return project;
        }

        int index = indexOpt.get();
        AlgNode input = project.getInput();
        if (!(input instanceof LogicalRelValues inputValues)) {
            return project;
        }

        if ( hasLessInputsThanColumns(project, inputValues)) {
            return createNewProjectWithIdentifiers(project, inputValues, index);
        }

        return visitChild(project, 0, project.getInput());
    }

    private Optional<Integer> findIdentifierIndex(LogicalRelProject project) {
        return project.getRowType().getFields().stream()
                .filter(field -> field.getName().equals(IdentifierUtils.IDENTIFIER_KEY))
                .map(AlgDataTypeField::getIndex)
                .findFirst();
    }

    private boolean hasLessInputsThanColumns(LogicalRelProject project, LogicalRelValues inputValues) {
        long fieldCount = project.getRowType().getFieldCount();
        long inputFieldCount = inputValues.tuples.get(0).size();
        return inputFieldCount < fieldCount;
    }

    private AlgNode createNewProjectWithIdentifiers(LogicalRelProject project, LogicalRelValues inputValues, int index) {
        ImmutableList<ImmutableList<RexLiteral>> immutableValues = adjustInputValues(inputValues, index);
        LogicalRelValues newInput = new LogicalRelValues(
                inputValues.getCluster(),
                inputValues.getTraitSet(),
                inputValues.getRowType(),
                immutableValues
        );

        List<RexNode> newProjects = createNewProjects(project, inputValues);
        return project.copy(project.getTraitSet(), newInput, newProjects, project.getRowType());
    }

    private ImmutableList<ImmutableList<RexLiteral>> adjustInputValues(LogicalRelValues inputValues, int index) {
        return inputValues.tuples.stream()
                .map(row -> {
                    PolyValue identifier = IdentifierUtils.getIdentifier();
                    ImmutableList.Builder<RexLiteral> builder = ImmutableList.builder();
                    builder.addAll(row.subList(0, index));
                    builder.add(new RexLiteral(
                            identifier,
                            AlgDataTypeFactoryImpl.DEFAULT.createPolyType(identifier.getType()),
                            PolyType.DECIMAL
                    ));
                    builder.addAll(row.subList(index, row.size()));
                    return builder.build();
                })
                .collect(ImmutableList.toImmutableList());
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
        switch ( modify.getOperation() ) {
            case UPDATE -> {
                if ( modify.getUpdateColumns().contains( IdentifierUtils.IDENTIFIER_KEY ) ) {
                    IdentifierUtils.throwIllegalFieldName();
                }
                return modify;
            }
            case INSERT -> {
                return visitChildren( modify );
            }
        }
        return modify;
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
