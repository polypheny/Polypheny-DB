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

package org.polypheny.db.adapter.neo4j;

import static org.polypheny.db.adapter.neo4j.util.NeoStatements.as_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.count_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.distinct_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.edge_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.labels_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.list_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.literal_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.match_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.node_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.path_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.quotedIdentifier_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.return_;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import org.polypheny.db.adapter.neo4j.rules.NeoGraphAlg;
import org.polypheny.db.adapter.neo4j.util.NeoStatements.NeoStatement;
import org.polypheny.db.adapter.neo4j.util.NeoStatements.OperatorStatement;
import org.polypheny.db.adapter.neo4j.util.NeoStatements.ReturnStatement;
import org.polypheny.db.adapter.neo4j.util.NeoStatements.StatementType;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.algebra.type.GraphType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.graph.PolyEdge.EdgeDirection;
import org.polypheny.db.type.entity.graph.PolyPath;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.util.Pair;


/**
 * Shuttle class, which saves the state of the graph Neo4j algebra nodes it passes through when needed.
 * This state is then later used to build the graph code ({@link org.apache.calcite.linq4j.tree.Expression}), which represents the passed algebra tree.
 */
public class NeoGraphImplementor extends AlgShuttleImpl {

    @Setter
    @Getter
    private boolean sorted;

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
    private final Set<String> matchedPaths = new HashSet<>();
    private ImmutableList<ImmutableList<RexLiteral>> values;


    public void visitChild( int ordinal, AlgNode input ) {
        assert ordinal == 0;
        ((NeoGraphAlg) input).implement( this );
        this.setLast( input );
    }


    private void addRowCount( NeoStatement count ) {
        statements.add( return_( as_( count, literal_( PolyString.of( "ROWCOUNT" ) ) ) ) );
    }


    public String build() {
        if ( !sorted ) {
            addReturnIfNecessary();
        }

        return statements.stream().map( NeoStatement::build ).collect( Collectors.joining( "\n" ) );
    }


    /**
     * Adds a specific Neo4j statement
     *
     * @param statement the statement to add
     */
    public void add( OperatorStatement statement ) {
        if ( statement.type == StatementType.WITH ) {
            matchedPaths.clear();
        }
        this.statements.add( statement );
    }


    public NeoStatement matchPath( PolyString name, PolyPath path ) {
        matchedPaths.add( name.value );
        return path_( name, path, PolyString.of( graph.mappingLabel ), false );
    }


    /**
     * If the finial statement is not a valid end command for cypher, this adds an appropriate <code>RETURN</code> statement.
     */
    public void addReturnIfNecessary() {
        if ( statements.isEmpty() && isAll ) {
            statements.add( match_(
                    node_( PolyString.of( "n" ), labels_( PolyString.of( this.graph.mappingLabel ) ) ),
                    path_( node_( "" ), edge_( PolyString.of( "e" ), this.graph.mappingLabel, list_( List.of() ), EdgeDirection.NONE ), node_( "" ) ) ) );
        }
        if ( statements.get( statements.size() - 1 ).type == StatementType.CREATE ) {
            addRowCount( literal_( PolyInteger.of( 1 ) ) );
        }

        OperatorStatement statement = statements.get( statements.size() - 1 );
        if ( statements.get( statements.size() - 1 ).type != StatementType.RETURN ) {
            if ( isDml ) {
                addRowCount( switch ( statement.type ) {
                    case SET, DELETE, DELETE_DETACH -> count_( "*" );
                    default -> literal_( PolyInteger.of( 1 ) );
                } );
            } else if ( statement.type == StatementType.WITH ) {
                // Reuse the translated projection so its aliases remain bound.
                statements.set( statements.size() - 1, return_( statement.statements ) );
            } else {
                // have to add
                statements.add( return_( getFields( last.getTupleType() ) ) );
            }
        }
    }


    /**
     * Returns the specified fields as cypher equivalent {@link NeoStatement}.
     *
     * @param rowType the fields to transform
     * @return the fields as a collection of {@link NeoStatement}
     */
    private List<NeoStatement> getFields( AlgDataType rowType ) {
        if ( isAll && rowType instanceof GraphType ) {
            return List.of( literal_( PolyString.of( "n" ) ), literal_( PolyString.of( "e" ) ) );
        }

        List<NeoStatement> statements = new ArrayList<>();
        for ( AlgDataTypeField field : rowType.getFields() ) {
            statements.add( getField( field ) );
        }

        return statements;
    }


    /**
     * Transforms a single field into a corresponding {@link NeoStatement}.
     *
     * @param field the field to transform
     * @return the field as Neo4j statement
     */
    private NeoStatement getField( AlgDataTypeField field ) {
        if ( field.getType().getPolyType() == PolyType.PATH ) {
            NeoStatement path = quotedIdentifier_( field.getName() );
            if ( !matchedPaths.contains( field.getName() ) || field.getType().getFields().isEmpty() ) {
                return path;
            }
            // The adapter decoder receives the actual path followed by its bindings in PathType field order.
            List<NeoStatement> values = new ArrayList<>();
            values.add( path );
            for ( AlgDataTypeField binding : field.getType().getFields() ) {
                values.add( quotedIdentifier_( binding.getName() ) );
            }
            return list_( values, "[", "]" );
        }
        return quotedIdentifier_( field.getName() );
    }


    /**
     * Returns queries which retrieve all nodes and all edges for the graph.
     *
     * @return a pair of node retrieval and edge retrieval queries
     */
    public Pair<String, String> getAllQueries() {
        String nodes = String.join( "\n", List.of(
                match_( node_( PolyString.of( "n" ), labels_( PolyString.of( graph.mappingLabel ) ) ) ).build(),
                return_( distinct_( literal_( PolyString.of( "n" ) ) ) ).build()
        ) );
        String edges = String.join(
                "\n",
                match_( path_( node_( PolyString.of( "n" ), labels_( PolyString.of( graph.mappingLabel ) ) ), edge_( "r" ), node_( "m" ) ) ).build(),
                return_( distinct_( literal_( PolyString.of( "r" ) ) ) ).build() );

        return Pair.of( nodes, edges );
    }


    /**
     * Sets values, if not already set.
     *
     * @param values the values to add
     */
    public void setValues( ImmutableList<ImmutableList<RexLiteral>> values ) {
        assert this.values == null : "only single lines of values can be used";
        this.values = values;
    }


    /**
     * Removes the top-most element for the {@link NeoStatement} stack if possible.
     *
     * @return the removed statement
     */
    public OperatorStatement removeLast() {
        assert !statements.isEmpty() : "Cannot remove neo statements from an empty stack.";
        OperatorStatement statement = statements.get( statements.size() - 1 );
        statements.remove( statements.size() - 1 );
        return statement;

    }


    /**
     * Replaces the last <code>RETURN</code> on the {@link NeoStatement} stack.
     *
     * @param return_ the new {@link NeoStatement} statement, which is added to the stack
     */
    public void replaceReturn( ReturnStatement return_ ) {
        Integer lastReturn = null;
        int i = 0;
        for ( OperatorStatement statement : statements ) {
            if ( statement instanceof ReturnStatement ) {
                lastReturn = i;
            }
            i++;
        }
        if ( lastReturn == null ) {
            throw new GenericRuntimeException( "Could not find a RETURN to replace" );
        }
        statements.set( lastReturn, return_ );

    }

}
