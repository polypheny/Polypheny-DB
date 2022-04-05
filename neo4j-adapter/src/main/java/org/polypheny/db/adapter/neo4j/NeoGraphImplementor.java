/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.adapter.neo4j;

import static org.polypheny.db.adapter.neo4j.util.NeoStatements.as_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.distinct_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.edge_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.labels_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.literal_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.match_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.node_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.path_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.return_;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.adapter.neo4j.rules.NeoGraphAlg;
import org.polypheny.db.adapter.neo4j.util.NeoStatements.ElementStatement;
import org.polypheny.db.adapter.neo4j.util.NeoStatements.NeoStatement;
import org.polypheny.db.adapter.neo4j.util.NeoStatements.OperatorStatement;
import org.polypheny.db.adapter.neo4j.util.NeoStatements.StatementType;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.type.PathType;
import org.polypheny.db.util.Pair;

public class NeoGraphImplementor extends AlgShuttleImpl {

    @Setter
    @Getter
    private AlgNode last;

    @Setter
    @Getter
    private NeoGraph graph;

    @Setter
    @Getter
    private boolean isDml;

    @Setter
    @Getter
    private boolean isAll;

    @Setter
    @Getter
    private boolean mapped = false;


    public final List<OperatorStatement> statements = new ArrayList<>();
    private ImmutableList<ImmutableList<RexLiteral>> values;


    public void visitChild( int ordinal, AlgNode input ) {
        assert ordinal == 0;
        ((NeoGraphAlg) input).implement( this );
        this.setLast( input );
    }


    private void addRowCount( int size ) {
        statements.add( return_( as_( literal_( size ), literal_( "ROWCOUNT" ) ) ) );
    }


    public String build() {
        addReturnIfNecessary();
        return statements.stream().map( NeoStatement::build ).collect( Collectors.joining( "\n" ) );
    }


    public void add( OperatorStatement statement ) {
        this.statements.add( statement );
    }


    public void addReturnIfNecessary() {
        if ( statements.get( statements.size() - 1 ).type == StatementType.CREATE ) {
            addRowCount( 1 );
        }

        OperatorStatement statement = statements.get( statements.size() - 1 );
        if ( statements.get( statements.size() - 1 ).type != StatementType.RETURN ) {
            if ( isDml ) {
                addRowCount( 1 );
            } else if ( statement.type == StatementType.WITH ) {
                // can replace
                statements.remove( statements.size() - 1 );
                statements.add( return_( statement.statements ) );
            } else {
                // have to add
                statements.add( return_( getFields( last.getRowType() ) ) );
            }
        }
    }


    private List<NeoStatement> getFields( AlgDataType rowType ) {
        List<NeoStatement> statements = new ArrayList<>();
        for ( AlgDataTypeField field : rowType.getFieldList() ) {
            statements.add( getField( field ) );
        }

        return statements;
    }


    private NeoStatement getField( AlgDataTypeField field ) {
        switch ( field.getType().getPolyType() ) {
            case NODE:
                return node_( field.getName() );
            case EDGE:
                return edge_( field.getName() );
            case PATH:
                PathType type = (PathType) field.getType();
                List<ElementStatement> path = new ArrayList<>();
                for ( AlgDataTypeField pathPart : type.getFieldList() ) {
                    path.add( (ElementStatement) getField( pathPart ) );
                }
                return path_( path );
        }
        return literal_( field.getName() );
    }


    public Pair<String, String> getAllQueries() {
        String nodes = String.join( "\n", List.of(
                match_( node_( "n", labels_( graph.mappingLabel ) ) ).build(),
                return_( distinct_( literal_( "n" ) ) ).build()
        ) );
        String edges = String.join(
                "\n",
                match_( path_( node_( "n", labels_( graph.mappingLabel ) ), edge_( "r" ), node_( "m" ) ) ).build(),
                return_( distinct_( literal_( "r" ) ) ).build() );

        return Pair.of( nodes, edges );
    }


    public void addValues( ImmutableList<ImmutableList<RexLiteral>> values ) {
        assert this.values == null : "only single lines of values can be used";
        this.values = values;
    }


    public OperatorStatement removeLast() {
        assert !statements.isEmpty() : "Cannot remove neo statements from an empty stack.";
        OperatorStatement statement = statements.get( statements.size() - 1 );
        statements.remove( statements.size() - 1 );
        return statement;

    }

}
