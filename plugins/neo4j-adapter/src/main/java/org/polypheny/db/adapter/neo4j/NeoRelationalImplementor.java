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
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.assign_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.create_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.delete_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.identityProperties_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.labels_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.list_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.literal_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.match_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.node_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.prepared_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.property_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.return_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.set_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.where_;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.linq4j.function.Function1;
import org.polypheny.db.adapter.neo4j.rules.NeoRelAlg;
import org.polypheny.db.adapter.neo4j.rules.relational.NeoFilter;
import org.polypheny.db.adapter.neo4j.rules.relational.NeoModify;
import org.polypheny.db.adapter.neo4j.rules.relational.NeoProject;
import org.polypheny.db.adapter.neo4j.types.NestedPolyType;
import org.polypheny.db.adapter.neo4j.util.NeoStatements;
import org.polypheny.db.adapter.neo4j.util.NeoStatements.ListStatement;
import org.polypheny.db.adapter.neo4j.util.NeoStatements.NeoStatement;
import org.polypheny.db.adapter.neo4j.util.NeoStatements.OperatorStatement;
import org.polypheny.db.adapter.neo4j.util.NeoStatements.PropertyStatement;
import org.polypheny.db.adapter.neo4j.util.NeoStatements.StatementType;
import org.polypheny.db.adapter.neo4j.util.NeoUtil;
import org.polypheny.db.adapter.neo4j.util.Translator;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.util.Pair;


/**
 * Shuttle class, which saves the state of the algebra Neo4j algebra nodes it passes through when needed.
 * This state is then later used to build the algebra code ({@link org.apache.calcite.linq4j.tree.Expression}), which represents the passed algebra tree.
 */
public class NeoRelationalImplementor extends AlgShuttleImpl {

    private final List<OperatorStatement> statements = new ArrayList<>();
    public NeoEntity selectFromTable = null;

    @Setter
    @Getter
    private boolean isPrepared;

    @Setter
    @Getter
    private boolean isDml;

    @Getter
    @Setter
    private NeoEntity entity;

    @Getter
    private final Map<Long, NestedPolyType> preparedTypes = new HashMap<>();


    private ImmutableList<ImmutableList<RexLiteral>> values;

    @Setter
    @Getter
    private AlgNode last;


    public void add( OperatorStatement statement ) {
        this.statements.add( statement );
    }


    public void addAll( List<OperatorStatement> statements ) {
        statements.forEach( this::add );
    }


    public void addValues( ImmutableList<ImmutableList<RexLiteral>> tuples ) {
        this.values = tuples;
    }


    public boolean hasValues() {
        return this.values != null && !this.values.isEmpty();
    }


    public boolean isEmpty() {
        return statements.isEmpty();
    }


    /**
     * Adds a cypher <code>CREATE</code> statement, which is used to map most SQL <code>DML</code> statements.
     */
    public void addCreate() {
        if ( values != null ) {
            Pair<Integer, OperatorStatement> res = createCreate( values, entity );
            add( res.right );
            addRowCount( res.left );
        } else if ( !statements.isEmpty() ) {
            add( create_( node_( PolyString.of( entity.name + "_" ), labels_( PolyString.of( entity.name ) ), identityProperties_( entity.getFields() ) ) ) );
            addRowCount( 1 );
        } else {
            throw new UnsupportedOperationException( "Neither values nor a source table was selected for the CREATE" );
        }

    }


    /**
     * Attaches a <code>RETURN</code> of the modified rows to the cypher statement stack.
     *
     * @param size the modified rows
     */
    private void addRowCount( int size ) {
        add( return_( as_( literal_( PolyInteger.of( size ) ), literal_( PolyString.of( "ROWCOUNT" ) ) ) ) );
    }


    private void addRowCountEntity() {
        add( return_( as_( literal_( PolyString.of( String.format( "COUNT(%s)", entity.name ) ) ), literal_( PolyString.of( "ROWCOUNT" ) ) ) ) );
    }


    /**
     * Attaches a cypher parameter value (<code>$param</code>), which is used to handle prepared values ({@link RexDynamicParam })
     */
    public void addPreparedValues() {
        if ( last instanceof NeoProject ) {
            add( createProjectValues( (NeoProject) last, entity, this ) );
            addRowCount( 1 );
            return;
        }
        throw new GenericRuntimeException( "" );
    }


    public void visitChild( int ordinal, AlgNode input ) {
        assert ordinal == 0;
        ((NeoRelAlg) input).implement( this );
        this.setLast( input );
    }


    /**
     * Attaches a <code>WITH</code> cypher statement which is used to map a SQL project in some cases.
     */
    public void addWith( NeoProject project, boolean isDml ) {
        add( create( NeoStatements::with_, project, last, this, isDml ) );
    }


    /**
     * Attaches a <code>CREATE</code> cypher statement which is used to map a SQL DML.
     */
    public static Pair<Integer, NeoStatements.OperatorStatement> createCreate( ImmutableList<ImmutableList<RexLiteral>> values, NeoEntity entity ) {
        int nodeI = 0;
        List<NeoStatements.NeoStatement> nodes = new ArrayList<>();
        AlgDataType rowType = entity.getTupleType();

        for ( ImmutableList<RexLiteral> row : values ) {
            int pos = 0;
            List<PropertyStatement> props = new ArrayList<>();
            for ( RexLiteral value : row ) {
                if ( pos >= rowType.getFieldCount() ) {
                    continue;
                }
                props.add( property_( rowType.getFields().get( pos ).getPhysicalName(), literal_( value ) ) );
                pos++;
            }
            String name = entity.name;

            nodes.add( node_( PolyString.of( name + nodeI ), NeoStatements.labels_( PolyString.of( name ) ), props ) );
            nodeI++;
        }

        return Pair.of( nodeI, NeoStatements.create_( nodes ) );
    }


    public static NeoStatements.OperatorStatement create( Function1<ListStatement<?>, OperatorStatement> transformer, NeoProject neoProject, AlgNode last, NeoRelationalImplementor implementor, boolean isDml ) {
        List<AlgDataTypeField> fields = neoProject.getTupleType().getFields();

        List<NeoStatements.NeoStatement> nodes = new ArrayList<>();
        int i = 0;
        for ( RexNode project : neoProject.getProjects() ) {
            Translator translator = new Translator( neoProject.getTupleType(), last.getTupleType(), new HashMap<>(), implementor, null, true );
            String res = project.accept( translator );
            assert res != null : "Unsupported operation encountered for projects in Neo4j.";

            nodes.add( as_( literal_( PolyString.of( res ) ), literal_( PolyString.of( fields.get( i ).getName() ) ) ) );
            i++;
        }
        // we add the original entity to no lose it for dmls
        if ( isDml ) {
            nodes.add( literal_( PolyString.of( implementor.entity.name ) ) );
        }

        return transformer.apply( list_( nodes ) );
    }


    public static OperatorStatement createProjectValues( NeoProject last, NeoEntity entity, NeoRelationalImplementor implementor ) {
        List<PropertyStatement> properties = new ArrayList<>();
        List<AlgDataTypeField> fields = entity.getTupleType().getFields();

        int i = 0;
        for ( RexNode project : last.getProjects() ) {
            String key = fields.get( i ).getPhysicalName();
            if ( project.isA( Kind.LITERAL ) ) {
                properties.add( property_( key, literal_( (RexLiteral) project ) ) );
            } else if ( project.isA( Kind.DYNAMIC_PARAM ) ) {
                implementor.addPreparedType( (RexDynamicParam) project );
                properties.add( property_( key, prepared_( (RexDynamicParam) project ) ) );
            } else {
                throw new UnsupportedOperationException( "This operation is not supported." );
            }
            i++;
        }
        String name = entity.name;

        return create_( node_( PolyString.of( name ), labels_( PolyString.of( name ) ), properties ) );
    }


    public void addPreparedType( RexDynamicParam dynamicParam ) {
        preparedTypes.put(
                dynamicParam.getIndex(),
                NestedPolyType.from( dynamicParam.getType() ) );
    }


    public String build() {
        addReturnIfNecessary();
        return statements.stream().map( NeoStatement::build ).collect( Collectors.joining( "\n" ) );

    }


    public void addReturnIfNecessary() {
        OperatorStatement statement = statements.get( statements.size() - 1 );
        if ( statements.get( statements.size() - 1 ).type != StatementType.RETURN ) {
            if ( isDml ) {
                addRowCountEntity();
            } else if ( statement.type == StatementType.WITH ) {
                // can replace
                statements.remove( statements.size() - 1 );
                statements.add( return_( statement.statements ) );
            } else {//if ( statement.type == StatementType.WHERE ) {
                // have to add
                statements.add( return_( list_( last.getTupleType().getFieldNames().stream().map( f -> literal_( PolyString.of( NeoUtil.fixParameter( f ) ) ) ).toList() ) ) );
            }
        }
    }


    public void addFilter( NeoFilter filter ) {
        Translator translator = new Translator( last.getTupleType(), last.getTupleType(), new HashMap<>(), this, null, true );
        add( where_( literal_( PolyString.of( filter.getCondition().accept( translator ) ) ) ) );
    }


    private Map<String, String> getToPhysicalMapping( @Nullable AlgNode node ) {
        Map<String, String> mapping = new HashMap<>();
        for ( AlgDataTypeField field : entity.getTupleType().getFields() ) {
            mapping.put( field.getName(), entity.name + "." + field.getPhysicalName() );
        }

        if ( node instanceof NeoProject project ) {
            for ( AlgDataTypeField field : project.getTupleType().getFields() ) {
                if ( !mapping.containsKey( field.getName() ) ) {
                    Translator translator = new Translator( project.getTupleType(), project.getTupleType(), new HashMap<>(), this, null, true );
                    mapping.put( field.getName(), project.getProjects().get( field.getIndex() ).accept( translator ) );
                }
            }
        }

        return mapping;
    }


    public void addDelete() {
        if ( isEmpty() ) {
            add( match_( node_( PolyString.of( entity.name ), labels_( PolyString.of( entity.name ) ) ) ) );
        }

        add( delete_( false, literal_( PolyString.of( entity.name ) ) ) );
    }


    public void addUpdate( NeoModify neoModify ) {
        Map<String, String> mapping = getToPhysicalMapping( last );

        List<NeoStatement> nodes = new ArrayList<>();
        int i = 0;
        for ( RexNode node : neoModify.getSourceExpressions() ) {
            Translator translator = new Translator( last.getTupleType(), last.getTupleType(), mapping, this, null, true );
            nodes.add( assign_( literal_( PolyString.of( mapping.get( neoModify.getUpdateColumns().get( i ) ) ) ), literal_( PolyString.of( node.accept( translator ) ) ) ) );
            i++;
        }

        add( set_( list_( nodes ) ) );
    }

}
