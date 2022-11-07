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

package org.polypheny.db.adapter.neo4j.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import org.polypheny.db.adapter.neo4j.util.NeoStatements.ElementStatement.ElementType;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.runtime.PolyCollections.PolyDictionary;
import org.polypheny.db.schema.graph.GraphObject;
import org.polypheny.db.schema.graph.PolyEdge;
import org.polypheny.db.schema.graph.PolyEdge.EdgeDirection;
import org.polypheny.db.schema.graph.PolyNode;
import org.polypheny.db.schema.graph.PolyPath;
import org.polypheny.db.type.PolyTypeFamily;

/**
 * Helper classes, which are used to create cypher queries with a object representation.
 */
public interface NeoStatements {

    enum StatementType {
        MATCH( "MATCH" ),
        CREATE( "CREATE" ),
        WHERE( "WHERE" ),
        RETURN( "RETURN" ),
        WITH( "WITH" ),
        SET( "SET" ),
        DELETE( "DELETE" ),
        DELETE_DETACH( "DETACH DELETE" ),
        UNWIND( "UNWIND" ),
        LIMIT( "LIMIT" ),
        SKIP( "SKIP" ),
        ORDER_BY( "ORDER BY" );

        public final String identifier;


        StatementType( String identifier ) {
            this.identifier = identifier;
        }
    }


    abstract class NeoStatement {

        public abstract String build();


        public final StatementType type;


        protected NeoStatement( StatementType type ) {
            this.type = type;
        }


        @Override
        public String toString() {
            return build();
        }


    }


    abstract class OperatorStatement extends NeoStatement {

        public final ListStatement<?> statements;


        protected OperatorStatement( StatementType type, ListStatement<?> statements ) {
            super( type );
            this.statements = statements;
        }


        @Override
        public String build() {
            return type.identifier + " " + statements.build();
        }


    }

    static <T extends NeoStatement> ListStatement<T> list_( List<T> statements ) {
        return list_( statements, "", "" );
    }

    static <T extends NeoStatement> ListStatement<T> list_( List<T> statements, String prefix, String postfix ) {
        return new ListStatement<>( statements, prefix, postfix );
    }

    class ListStatement<T extends NeoStatement> extends NeoStatement {

        private final List<T> statements;
        private final String prefix;
        private final String postfix;


        protected ListStatement( List<T> statements, String prefix, String postfix ) {
            super( null );
            this.statements = statements;
            this.prefix = prefix;
            this.postfix = postfix;
        }


        public int size() {
            return statements.size();
        }


        public boolean isEmpty() {
            return statements.isEmpty();
        }


        @Override
        public String build() {
            if ( statements.isEmpty() ) {
                return "";
            }
            return prefix + this.statements.stream().map( NeoStatement::build ).collect( Collectors.joining( ", " ) ) + postfix;
        }


    }


    abstract class ElementStatement extends NeoStatement {


        protected ElementStatement() {
            super( null );
        }


        public abstract ElementType getType();


        enum ElementType {
            EDGE,
            NODE
        }

    }


    class NodeStatement extends ElementStatement {

        private final String identifier;
        private final LabelsStatement labels;
        private final ListStatement<?> properties;

        @Getter
        private final ElementType type = ElementType.NODE;


        protected NodeStatement( String identifier, LabelsStatement labels, ListStatement<?> properties ) {
            this.identifier = identifier == null ? "" : identifier;
            this.labels = labels;
            this.properties = properties;
        }


        @Override
        public String build() {
            return String.format( "( %s%s %s )", identifier, labels.build(), properties.build() );
        }


    }

    static NodeStatement node_( String identifier, LabelsStatement labels, PropertyStatement... properties ) {
        return new NodeStatement( identifier, labels, list_( Arrays.asList( properties ), "{", "}" ) );
    }

    static NodeStatement node_( String identifier, LabelsStatement labels, List<PropertyStatement> properties ) {
        return new NodeStatement( identifier, labels, list_( properties, "{", "}" ) );
    }

    static NodeStatement node_( String identifier ) {
        return node_( identifier, labels_(), List.of() );
    }

    static NodeStatement node_( PolyNode node, String mappingLabel, boolean addId ) {
        List<PropertyStatement> statements = new ArrayList<>( properties_( node.properties ) );
        if ( addId ) {
            statements.add( property_( "_id", string_( (node.id) ) ) );
        }
        ArrayList<String> labels = new ArrayList<>( node.labels );
        if ( mappingLabel != null ) {
            labels.add( mappingLabel );
        }
        String defIdentifier = node.getVariableName();

        return node_( defIdentifier, labels_( labels ), statements );
    }


    class EdgeStatement extends ElementStatement {

        private final String identifier;
        private final LabelsStatement label;
        private final ListStatement<PropertyStatement> properties;
        private final EdgeDirection direction;
        private final String range;

        @Getter
        private final ElementType type = ElementType.EDGE;


        protected EdgeStatement( @Nullable String identifier, String range, LabelsStatement labelsStatement, ListStatement<PropertyStatement> properties, EdgeDirection direction ) {
            this.identifier = identifier == null ? "" : identifier;
            assert labelsStatement.labels.size() <= 1 : "Edges only allow one label.";
            this.label = labelsStatement;
            this.properties = properties;
            this.direction = direction;
            this.range = range;
        }


        @Override
        public String build() {
            String statement = String.format( "-[%s%s%s %s]-", identifier, label.build(), range, properties.build() );
            if ( direction == EdgeDirection.LEFT_TO_RIGHT ) {
                statement = statement + ">";
            } else if ( direction == EdgeDirection.RIGHT_TO_LEFT ) {
                statement = statement + "<";
            }
            return statement;
        }


    }

    static EdgeStatement edge_( @Nullable String identifier, String range, List<String> labels, ListStatement<PropertyStatement> properties, EdgeDirection direction ) {
        return new EdgeStatement( identifier, range, labels_( labels ), properties, direction );
    }

    static EdgeStatement edge_( @Nullable String identifier, String label, ListStatement<PropertyStatement> properties, EdgeDirection direction ) {
        return new EdgeStatement( identifier, "", labels_( label ), properties, direction );
    }

    static EdgeStatement edge_( @Nullable String identifier ) {
        return new EdgeStatement( identifier, "", labels_(), list_( List.of() ), EdgeDirection.NONE );
    }

    static EdgeStatement edge_( PolyEdge edge, boolean addId ) {
        List<PropertyStatement> props = new ArrayList<>( properties_( edge.properties ) );
        if ( addId ) {
            props.add( property_( "_id", string_( edge.id ) ) );
            props.add( property_( "__sourceId__", string_( edge.source ) ) );
            props.add( property_( "__targetId__", string_( edge.target ) ) );
        }
        String defIdentifier = edge.getVariableName();

        return edge_( defIdentifier, edge.getRangeDescriptor(), edge.labels, list_( props, "{", "}" ), edge.direction );
    }

    class PathStatement extends NeoStatement {

        private final List<ElementStatement> pathElements;
        private final String identifier;


        protected PathStatement( String identifier, List<ElementStatement> pathElements ) {
            super( null );
            this.pathElements = fixPath( pathElements );
            this.identifier = identifier;
        }


        private List<ElementStatement> fixPath( List<ElementStatement> initial ) {
            List<ElementStatement> paths = new LinkedList<>();
            ElementType lastType = ElementType.EDGE;
            for ( ElementStatement element : initial ) {
                switch ( element.getType() ) {
                    case EDGE:
                        if ( lastType != ElementType.NODE ) {
                            paths.add( node_( null ) );
                        }
                        break;
                    case NODE:
                        if ( lastType != ElementType.EDGE ) {
                            paths.add( edge_( null ) );
                        }
                        break;
                }
                paths.add( element );
                lastType = element.getType();
            }
            return paths;
        }


        @Override
        public String build() {
            String namedPath = identifier == null || identifier.contains( "$" ) ? "" : String.format( "%s = ", identifier );
            return namedPath + pathElements.stream().map( NeoStatement::build ).collect( Collectors.joining() );
        }

    }

    static PathStatement path_( ElementStatement... elements ) {
        return new PathStatement( null, Arrays.asList( elements ) );
    }

    static PathStatement path_( List<ElementStatement> elements ) {
        return new PathStatement( null, elements );
    }


    static PathStatement path_( @Nullable String identifier, PolyPath path, @Nullable String mappingLabel, boolean addId ) {
        int i = 0;
        List<ElementStatement> elements = new ArrayList<>();
        for ( GraphObject object : path.getPath() ) {
            elements.add( i % 2 == 0
                    ? node_( (PolyNode) object, mappingLabel, addId )
                    : edge_( (PolyEdge) object, addId ) );
            i++;
        }

        String name = path.getVariableName() == null ? identifier : path.getVariableName();

        return new PathStatement( name, elements );
    }


    class PropertyStatement extends NeoStatement {

        private final String key;
        private final NeoStatement value;


        protected PropertyStatement( String key, NeoStatement value ) {
            super( null );
            this.key = key;
            this.value = value;
        }


        @Override
        public String build() {
            return key + ":" + value.build();
        }


    }

    static PropertyStatement property_( String key, NeoStatement value ) {
        return new PropertyStatement( key, value );
    }

    static List<PropertyStatement> properties_( PolyDictionary properties ) {
        List<PropertyStatement> props = new ArrayList<>();
        for ( Entry<String, Object> entry : properties.entrySet() ) {
            props.add( property_( entry.getKey(), _literalOrString( entry.getValue() ) ) );
        }
        return props;
    }

    static NeoStatement _literalOrString( Object value ) {
        if ( value instanceof String ) {
            return string_( value );
        } else if ( value instanceof List ) {
            return literal_( String.format( "[%s]", ((List<Object>) value).stream().map( value1 -> _literalOrString( value1 ).build() ).collect( Collectors.joining( ", " ) ) ) );
        } else {
            return literal_( value );
        }
    }

    class LabelsStatement extends NeoStatement {

        private final List<String> labels;


        protected LabelsStatement( List<String> labels ) {
            super( null );
            this.labels = labels;
        }


        @Override
        public String build() {
            return labels.stream().map( l -> ":" + l ).collect( Collectors.joining() );
        }


    }

    static LabelsStatement labels_( String... labels ) {
        return new LabelsStatement( Arrays.asList( labels ) );
    }

    static LabelsStatement labels_( List<String> labels ) {
        return new LabelsStatement( labels );
    }

    class AssignStatement extends NeoStatement {


        private final NeoStatement source;
        private final NeoStatement target;


        protected AssignStatement( NeoStatement source, NeoStatement target ) {
            super( null );
            this.source = source;
            this.target = target;
        }


        @Override
        public String build() {
            return source.build() + " = " + target.build();
        }


    }

    static AssignStatement assign_( NeoStatement target, NeoStatement source ) {
        return new AssignStatement( target, source );
    }


    class LiteralStatement extends NeoStatement {

        private final String value;


        protected LiteralStatement( String value ) {
            super( null );
            this.value = value;
        }


        @Override
        public String build() {
            return value;
        }


    }

    static LiteralStatement literal_( Object value ) {
        return new LiteralStatement( value == null ? null : value.toString() );
    }

    static LiteralStatement string_( Object value ) {
        return new LiteralStatement( value == null ? null : "'" + value + "'" );
    }

    static LiteralStatement literal_( RexLiteral literal ) {
        String prePostFix = "";
        if ( PolyTypeFamily.CHARACTER.contains( literal.getType() ) ) {
            prePostFix = "\"";
        }
        return literal_( String.format( "%s%s%s",
                prePostFix,
                NeoUtil.rexAsString( literal, null, false ),
                prePostFix ) );

    }

    class DistinctStatement extends NeoStatement {

        private final NeoStatement statement;


        protected DistinctStatement( NeoStatement statement ) {
            super( null );
            this.statement = statement;
        }


        @Override
        public String build() {
            return " DISTINCT " + statement.build();
        }

    }

    static DistinctStatement distinct_( NeoStatement statement ) {
        return new DistinctStatement( statement );
    }

    class AsStatement extends NeoStatement {

        private final NeoStatement key;
        private final NeoStatement alias;


        protected AsStatement( NeoStatement key, NeoStatement alias ) {
            super( null );
            this.key = key;
            this.alias = alias;
        }


        @Override
        public String build() {
            return key.build() + " AS " + NeoUtil.fixParameter( alias.build() );
        }


    }

    static AsStatement as_( NeoStatement key, NeoStatement value ) {
        return new AsStatement( key, value );
    }


    class PreparedStatement extends NeoStatement {

        private final long index;


        protected PreparedStatement( long index ) {
            super( null );
            this.index = index;
        }


        @Override
        public String build() {
            return NeoUtil.asParameter( index, true );
        }


    }

    static PreparedStatement prepared_( long index ) {
        return new PreparedStatement( index );
    }

    static PreparedStatement prepared_( RexDynamicParam param ) {
        return new PreparedStatement( param.getIndex() );
    }


    class CreateStatement extends OperatorStatement {

        protected CreateStatement( ListStatement<?> statements ) {
            super( StatementType.CREATE, statements );
        }

    }

    static CreateStatement create_( NeoStatement... statement ) {
        return new CreateStatement( list_( Arrays.asList( statement ) ) );
    }

    static CreateStatement create_( List<NeoStatement> statement ) {
        return new CreateStatement( list_( statement ) );
    }

    class UnwindStatement extends OperatorStatement {

        protected UnwindStatement( NeoStatement statement ) {
            super( StatementType.UNWIND, list_( List.of( statement ) ) );
        }

    }

    static UnwindStatement unwind_( NeoStatement statement ) {
        return new UnwindStatement( statement );
    }

    class ReturnStatement extends OperatorStatement {

        protected ReturnStatement( ListStatement<?> statements ) {
            super( StatementType.RETURN, statements );
        }

    }

    static ReturnStatement return_( NeoStatement... statement ) {
        return new ReturnStatement( list_( Arrays.asList( statement ) ) );
    }

    static ReturnStatement return_( List<NeoStatement> statements ) {
        return new ReturnStatement( list_( statements ) );
    }


    class WhereStatement extends OperatorStatement {

        protected WhereStatement( ListStatement<?> statements ) {
            super( StatementType.WHERE, statements );
        }

    }

    static WhereStatement where_( NeoStatement... statement ) {
        return new WhereStatement( list_( Arrays.asList( statement ) ) );
    }


    class MatchStatement extends OperatorStatement {

        protected MatchStatement( ListStatement<?> statements ) {
            super( StatementType.MATCH, statements );
        }

    }

    static MatchStatement match_( NeoStatement... statement ) {
        return new MatchStatement( list_( Arrays.asList( statement ) ) );
    }


    class WithStatement extends OperatorStatement {

        protected WithStatement( ListStatement<?> statements ) {
            super( StatementType.WITH, statements );
        }

    }

    static WithStatement with_( NeoStatement... statement ) {
        return new WithStatement( list_( Arrays.asList( statement ) ) );
    }

    class SetStatement extends OperatorStatement {

        protected SetStatement( ListStatement<?> statements ) {
            super( StatementType.SET, statements );
        }

    }

    static SetStatement set_( NeoStatement... statement ) {
        return new SetStatement( list_( Arrays.asList( statement ) ) );
    }

    class DeleteStatement extends OperatorStatement {

        protected DeleteStatement( boolean detach, ListStatement<?> statements ) {
            super( detach ? StatementType.DELETE_DETACH : StatementType.DELETE, statements );
        }

    }

    static DeleteStatement delete_( boolean detach, NeoStatement... statement ) {
        return new DeleteStatement( detach, list_( Arrays.asList( statement ) ) );
    }

    class LimitStatement extends OperatorStatement {

        protected LimitStatement( NeoStatement statement ) {
            super( StatementType.LIMIT, list_( List.of( statement ) ) );
        }

    }

    static LimitStatement limit_( int limit ) {
        return new LimitStatement( literal_( limit ) );
    }

    class SkipStatement extends OperatorStatement {

        protected SkipStatement( NeoStatement statement ) {
            super( StatementType.SKIP, list_( List.of( statement ) ) );
        }

    }

    static SkipStatement skip_( int offset ) {
        return new SkipStatement( literal_( offset ) );
    }

    class OrderByStatement extends OperatorStatement {

        protected OrderByStatement( ListStatement<?> statements ) {
            super( StatementType.ORDER_BY, statements );
        }

    }

    static OrderByStatement orderBy_( ListStatement<?> statements ) {
        return new OrderByStatement( statements );
    }

}
