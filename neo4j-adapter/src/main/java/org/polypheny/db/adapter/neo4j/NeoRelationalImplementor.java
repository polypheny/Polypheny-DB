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
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.assign_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.create_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.delete_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.labels_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.list_;
import static org.polypheny.db.adapter.neo4j.util.NeoStatements.literal_;
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
import org.polypheny.db.adapter.neo4j.util.NeoStatements;
import org.polypheny.db.adapter.neo4j.util.NeoStatements.ListStatement;
import org.polypheny.db.adapter.neo4j.util.NeoStatements.NeoStatement;
import org.polypheny.db.adapter.neo4j.util.NeoStatements.OperatorStatement;
import org.polypheny.db.adapter.neo4j.util.NeoStatements.PropertyStatement;
import org.polypheny.db.adapter.neo4j.util.NeoStatements.StatementType;
import org.polypheny.db.adapter.neo4j.util.NeoUtil;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.AlgShuttleImpl;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.plan.AlgOptTable;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexLocalRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexVisitorImpl;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

public class NeoRelationalImplementor extends AlgShuttleImpl {

    private final List<OperatorStatement> statements = new ArrayList<>();

    @Setter
    @Getter
    private boolean isPrepared;

    @Setter
    @Getter
    private boolean isDml;

    @Getter
    private AlgOptTable table;

    @Getter
    private NeoEntity entity;

    @Getter
    private final Map<Long, Pair<PolyType, PolyType>> preparedTypes = new HashMap<>();


    private ImmutableList<ImmutableList<RexLiteral>> values;

    @Setter
    @Getter
    private AlgNode last;


    public void add( OperatorStatement statement ) {
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
        Pair<Integer, OperatorStatement> res = createCreate( values, entity );
        add( res.right );
        addRowCount( res.left );
    }


    private void addRowCount( int size ) {
        add( return_( as_( literal_( size ), literal_( "ROWCOUNT" ) ) ) );
    }


    public void addPreparedValues() {
        if ( last instanceof NeoProject ) {
            add( createProjectValues( (NeoProject) last, entity, this ) );
            addRowCount( 1 );
            return;
        }
        throw new RuntimeException();
    }


    public void visitChild( int ordinal, AlgNode input ) {
        assert ordinal == 0;
        ((NeoRelAlg) input).implement( this );
        this.setLast( input );
    }


    public void addWith( NeoProject project ) {
        add( create( NeoStatements::with_, project, last, this ) );
    }


    public static Pair<Integer, NeoStatements.OperatorStatement> createCreate( ImmutableList<ImmutableList<RexLiteral>> values, NeoEntity entity ) {
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


    public static NeoStatements.OperatorStatement create( Function1<ListStatement<?>, OperatorStatement> transformer, NeoProject neoProject, AlgNode last, NeoRelationalImplementor implementor ) {
        List<AlgDataTypeField> fields = neoProject.getRowType().getFieldList();

        List<NeoStatements.NeoStatement> nodes = new ArrayList<>();
        int i = 0;
        for ( RexNode project : neoProject.getProjects() ) {
            Translator translator = new Translator( last.getRowType(), new HashMap<>(), implementor );
            String res = project.accept( translator );
            assert res != null : "Unsupported operation encountered for projects in Neo4j.";

            nodes.add( as_( literal_( res ), literal_( fields.get( i ).getName() ) ) );
            i++;
        }

        return transformer.apply( list_( nodes ) );
    }


    public static OperatorStatement createProjectValues( NeoProject last, NeoEntity entity, NeoRelationalImplementor implementor ) {
        List<PropertyStatement> properties = new ArrayList<>();
        List<AlgDataTypeField> fields = entity.getRowType( entity.getTypeFactory() ).getFieldList();

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
        String name = entity.phsicalEntityName;

        return create_( node_( name, labels_( name ), properties ) );
    }


    private void addPreparedType( RexDynamicParam dynamicParam ) {
        preparedTypes.put(
                dynamicParam.getIndex(),
                Pair.of( dynamicParam.getType().getPolyType(), NeoUtil.getComponentTypeOrParent( dynamicParam.getType() ) ) );
    }


    public String build() {
        addReturnIfNecessary();
        return statements.stream().map( NeoStatement::build ).collect( Collectors.joining( "\n" ) );

    }


    public void addReturnIfNecessary() {
        OperatorStatement statement = statements.get( statements.size() - 1 );
        if ( statements.get( statements.size() - 1 ).type != StatementType.RETURN ) {
            if ( isDml ) {
                addRowCount( 1 );
            } else if ( statement.type == StatementType.WITH ) {
                // can replace
                statements.remove( statements.size() - 1 );
                statements.add( return_( statement.statements ) );
            } else {//if ( statement.type == StatementType.WHERE ) {
                // have to add
                statements.add( return_( list_( last.getRowType().getFieldNames().stream().map( f -> literal_( NeoUtil.fixParameter( f ) ) ).collect( Collectors.toList() ) ) ) );
            }
        }
    }


    public void addFilter( NeoFilter filter ) {
        Translator translator = new Translator( last.getRowType(), isDml ? getToPhysicalMapping( null ) : new HashMap<>(), this );
        add( where_( literal_( filter.getCondition().accept( translator ) ) ) );
    }


    private Map<String, String> getToPhysicalMapping( @Nullable NeoProject project ) {
        Map<String, String> mapping = new HashMap<>();
        for ( AlgDataTypeField field : table.getRowType().getFieldList() ) {
            mapping.put( field.getName(), entity.phsicalEntityName + "." + field.getPhysicalName() );
        }

        if ( project != null ) {
            for ( AlgDataTypeField field : project.getRowType().getFieldList() ) {
                if ( !mapping.containsKey( field.getName() ) ) {
                    Translator translator = new Translator( project.getRowType(), new HashMap<>(), this );
                    mapping.put( field.getName(), project.getProjects().get( field.getIndex() ).accept( translator ) );
                }
            }
        }

        return mapping;
    }


    public void addDelete() {
        add( delete_( literal_( entity.phsicalEntityName ) ) );
    }


    public void addUpdate( NeoModify neoModify ) {
        NeoProject project = (NeoProject) last;

        Map<String, String> mapping = getToPhysicalMapping( project );

        List<NeoStatement> nodes = new ArrayList<>();
        int i = 0;
        for ( RexNode node : neoModify.getSourceExpressionList() ) {
            Translator translator = new Translator( last.getRowType(), mapping, this );
            nodes.add( assign_( literal_( mapping.get( neoModify.getUpdateColumnList().get( i ) ) ), literal_( node.accept( translator ) ) ) );
            i++;
        }

        add( set_( list_( nodes ) ) );
    }


    private static class Translator extends RexVisitorImpl<String> {


        private final List<AlgDataTypeField> fields;
        private final Map<String, String> mapping;
        private final NeoRelationalImplementor implementor;


        protected Translator( AlgDataType rowType, Map<String, String> mapping, NeoRelationalImplementor implementor ) {
            super( true );
            this.fields = rowType.getFieldList();
            this.mapping = mapping;
            this.implementor = implementor;
        }


        @Override
        public String visitLiteral( RexLiteral literal ) {
            return NeoUtil.rexAsString( literal );
        }


        @Override
        public String visitInputRef( RexInputRef inputRef ) {
            String name = fields.get( inputRef.getIndex() ).getName();
            if ( mapping.containsKey( name ) ) {
                return mapping.get( name );
            }
            return name;
        }


        @Override
        public String visitLocalRef( RexLocalRef localRef ) {
            String name = fields.get( localRef.getIndex() ).getName();
            if ( mapping.containsKey( name ) ) {
                return mapping.get( name );
            }
            return name;
        }


        @Override
        public String visitDynamicParam( RexDynamicParam dynamicParam ) {
            implementor.addPreparedType( dynamicParam );
            return NeoUtil.asParameter( dynamicParam.getIndex(), true );
        }


        @Override
        public String visitCall( RexCall call ) {
            List<String> ops = call.operands.stream().map( o -> o.accept( this ) ).collect( Collectors.toList() );

            Function1<List<String>, String> getter = NeoUtil.getOpAsNeo( call.op.getOperatorName() );
            assert getter != null : "Function is not supported by the Neo4j adapter.";
            return "(" + getter.apply( ops ) + ")";
        }

    }

}
