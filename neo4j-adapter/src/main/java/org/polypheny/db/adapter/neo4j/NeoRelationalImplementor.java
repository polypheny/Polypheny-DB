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
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.create_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.labels_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.list_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.literal_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.node_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.prepared_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.property_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.return_;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.linq4j.tree.Expression;
import org.polypheny.db.adapter.enumerable.EnumUtils;
import org.polypheny.db.adapter.neo4j.rules.NeoAlg;
import org.polypheny.db.adapter.neo4j.rules.NeoProject;
import org.polypheny.db.adapter.neo4j.util.NeoStatements;
import org.polypheny.db.adapter.neo4j.util.NeoStatements.NeoStatement;
import org.polypheny.db.adapter.neo4j.util.NeoStatements.PropertyStatement;
import org.polypheny.db.adapter.neo4j.util.NeoUtil;
import org.polypheny.db.adapter.neo4j.util.TypeFrame;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.Pair;

public class NeoRelationalImplementor extends AlgShuttleImpl {

    public static final List<Pair<String, String>> ROWCOUNT = List.of( Pair.of( null, "ROWCOUNT" ) );
    private final List<NeoStatements.NeoStatement> statements = new ArrayList<>();
    public boolean isPrepared;
    @Getter
    private TypeFrame frame;

    @Getter
    private AlgOptTable table;

    @Getter
    private NeoEntity entity;


    private ImmutableList<ImmutableList<RexLiteral>> values;

    @Setter
    @Getter
    private AlgNode last;


    public void add( NeoStatements.NeoStatement statement ) {
        this.statements.add( statement );
    }


    public void setTable( AlgOptTable table ) {
        this.table = table;
        this.entity = (NeoEntity) table.getTable();
    }


    public void addValues( ImmutableList<ImmutableList<RexLiteral>> tuples ) {
        this.values = tuples;
    }


    public boolean hasValues() {
        return this.values != null && !this.values.isEmpty();
    }


    public void addCreate() {
        Pair<Integer, NeoStatement> res = createCreate( values, entity );
        add( res.right );
        addRowCount( res.left );
    }


    private void addRowCount( int size ) {
        add( return_( as_( literal_( size ), literal_( "ROWCOUNT" ) ) ) );
    }


    public void addPreparedValues() {
        if ( last instanceof NeoProject ) {
            add( createProjectValues( (NeoProject) last, entity ) );
            addRowCount( 1 );
            return;
        }
        throw new RuntimeException();
    }


    public Expression asExpression() {
        return EnumUtils.constantArrayList( statements
                .stream()
                .map( NeoStatements.NeoStatement::asExpression )
                .collect( Collectors.toList() ), NeoStatements.NeoStatement.class );
    }


    public void visitChild( int ordinal, AlgNode input ) {
        assert ordinal == 0;
        ((NeoAlg) input).implement( this );
        this.setLast( input );
    }


    public void addReturn( NeoProject project ) {
        add( createReturn( project, entity ) );
    }


    public static Pair<Integer, NeoStatements.NeoStatement> createCreate( ImmutableList<ImmutableList<RexLiteral>> values, NeoEntity entity ) {
        int nodeI = 0;
        List<NeoStatements.NeoStatement> nodes = new ArrayList<>();
        AlgDataType rowType = entity.getRowType( entity.getTypeFactory() );

        for ( ImmutableList<RexLiteral> row : values ) {
            int i = 0;
            List<PropertyStatement> props = new ArrayList<>();
            for ( RexLiteral value : row ) {
                props.add( property_( rowType.getFieldList().get( i ).getPhysicalName(), literal_( NeoUtil.rexAsString( value ) ) ) );
                i++;
            }
            nodes.add( NeoStatements.node_( String.valueOf( nodeI ), NeoStatements.labels_( entity.phsicalEntityName ), props ) );
            nodeI++;
        }

        return Pair.of( nodeI, NeoStatements.create_( nodes ) );
    }


    public static NeoStatements.NeoStatement createReturn( NeoProject neoProject, NeoEntity entity ) {
        List<AlgDataTypeField> fields = entity.getRowType( entity.getTypeFactory() ).getFieldList();

        List<NeoStatements.NeoStatement> nodes = new ArrayList<>();
        int i = 0;
        for ( RexNode project : neoProject.getProjects() ) {
            String key = fields.get( i ).getName();
            NeoStatement statement;
            if ( project.isA( Kind.LITERAL ) ) {
                statement = literal_( (RexLiteral) project );
            } else if ( project.isA( Kind.DYNAMIC_PARAM ) ) {
                statement = prepared_( (RexDynamicParam) project );
            } else if ( project.isA( Kind.INPUT_REF ) ) {
                statement = literal_( fields.get( i ).getName() );
            } else {
                throw new UnsupportedOperationException( "This operation is not supported." );
            }
            nodes.add( as_( statement, literal_( fields.get( i ).getName() ) ) );
            i++;
        }

        return return_( list_( nodes ) );
    }


    public static NeoStatement createProjectValues( NeoProject last, NeoEntity entity ) {
        List<PropertyStatement> properties = new ArrayList<>();
        List<AlgDataTypeField> fields = entity.getRowType( entity.getTypeFactory() ).getFieldList();

        int i = 0;
        for ( RexNode project : last.getProjects() ) {
            String key = fields.get( i ).getPhysicalName();
            if ( project.isA( Kind.LITERAL ) ) {
                properties.add( property_( key, literal_( (RexLiteral) project ) ) );
            } else if ( project.isA( Kind.DYNAMIC_PARAM ) ) {
                properties.add( property_( key, prepared_( (RexDynamicParam) project ) ) );
            } else {
                throw new UnsupportedOperationException( "This operation is not supported." );
            }
            i++;
        }
        String name = entity.phsicalEntityName;

        return create_( node_( name, labels_( name ), properties ) );
    }


    public String build() {
        return statements.stream().map( NeoStatement::build ).collect( Collectors.joining( "\n" ) );

    }

}
