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
import org.polypheny.db.catalog.entity.physical.PhysicalField;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.type.PolyTypeFamily;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.category.PolyNumber;
import org.polypheny.db.type.entity.graph.GraphObject;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.graph.PolyEdge;
import org.polypheny.db.type.entity.graph.PolyEdge.EdgeDirection;
import org.polypheny.db.type.entity.graph.PolyNode;
import org.polypheny.db.type.entity.graph.PolyPath;

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

        FOREACH( "FOREACH" ),
        DELETE( "DELETE" ),
        DELETE_DETACH( "DETACH DELETE" ),
        UNWIND( "UNWIND" ),
        LIMIT( "LIMIT" ),
        SKIP( "SKIP" ),
        ORDER_BY( "ORDER BY" ),
        AGGREGATE( "AGGREGATE" );

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
            NODE,
            COLLECTION
        }

    }


    class CollectionStatement extends ElementStatement {

        private final String identifier;
        private final String wrapper;


        protected CollectionStatement( String wrapper, String identifier ) {
            this.identifier = identifier == null ? "" : identifier;
            this.wrapper = wrapper;
        }


        @Override
        public String build() {
            return String.format( "%s( %s )", wrapper, identifier );
        }


        @Override
        public ElementType getType() {
            return ElementType.COLLECTION;
        }

    }

    static CollectionStatement nodes_( String identifier ) {
        return new CollectionStatement( "nodes", identifier );
    }

    static CollectionStatement relationships_( String identifier ) {
        return new CollectionStatement( "relationships", identifier );
    }

    static CollectionStatement properties_( String identifier ) {
        return new CollectionStatement( "properties", identifier );
    }


    class NodeStatement extends ElementStatement {

        private final PolyString identifier;
        private final LabelsStatement labels;
        private final ListStatement<?> properties;

        @Getter
        private final ElementType type = ElementType.NODE;


        protected NodeStatement( PolyString identifier, LabelsStatement labels, ListStatement<?> properties ) {
            this.identifier = identifier == null || identifier.isNull() ? PolyString.of( "" ) : identifier;
            this.labels = labels;
            this.properties = properties;
        }


        @Override
        public String build() {
            return String.format( "( %s%s %s )", identifier, labels.build(), properties.build() );
        }


    }


    static NodeStatement node_( PolyString identifier, LabelsStatement labels, PropertyStatement... properties ) {
        return new NodeStatement( identifier, labels, list_( Arrays.asList( properties ), "{", "}" ) );
    }

    static NodeStatement node_( PolyString identifier, LabelsStatement labels, List<PropertyStatement> properties ) {
        return new NodeStatement( identifier, labels, list_( properties, "{", "}" ) );
    }

    static NodeStatement node_( PolyString identifier ) {
        return node_( identifier, labels_(), List.of() );
    }

    static NodeStatement node_( String identifier ) {
        return node_( PolyString.of( identifier ), labels_(), List.of() );
    }

    static NodeStatement node_( PolyNode node, PolyString mappingLabel, boolean addId ) {
        List<PropertyStatement> statements = new ArrayList<>( properties_( node.properties ) );
        if ( addId ) {
            statements.add( property_( PolyString.of( "_id" ), string_( (node.id) ) ) );
        }
        List<PolyString> labels = PolyList.copyOf( node.labels );
        if ( mappingLabel != null ) {
            labels.add( mappingLabel );
        }
        PolyString defIdentifier = node.getVariableName();

        return node_( defIdentifier, labels_( labels ), statements );
    }


    class EdgeStatement extends ElementStatement {

        private final PolyString identifier;
        private final LabelsStatement label;
        private final ListStatement<PropertyStatement> properties;
        private final EdgeDirection direction;
        private final PolyString range;

        @Getter
        private final ElementType type = ElementType.EDGE;


        protected EdgeStatement( @Nullable PolyString identifier, PolyString range, LabelsStatement labelsStatement, ListStatement<PropertyStatement> properties, EdgeDirection direction ) {
            this.identifier = identifier == null || identifier.isNull() ? PolyString.of( "" ) : identifier;
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
                statement = "<" + statement;
            }
            return statement;
        }


    }

    static EdgeStatement edge_( @Nullable PolyString identifier, PolyString range, List<PolyString> labels, ListStatement<PropertyStatement> properties, EdgeDirection direction ) {
        return new EdgeStatement( identifier, range, labels_( labels ), properties, direction );
    }

    static EdgeStatement edge_( @Nullable PolyString identifier, String label, ListStatement<PropertyStatement> properties, EdgeDirection direction ) {
        return new EdgeStatement( identifier, PolyString.of( "" ), labels_( PolyString.of( label ) ), properties, direction );
    }

    static EdgeStatement edge_( @Nullable String identifier ) {
        return new EdgeStatement( PolyString.of( identifier ), PolyString.of( "" ), labels_(), list_( List.of() ), EdgeDirection.NONE );
    }

    static EdgeStatement edge_( PolyEdge edge, boolean addId ) {
        List<PropertyStatement> props = new ArrayList<>( properties_( edge.properties ) );
        if ( addId ) {
            props.add( property_( PolyString.of( "_id" ), string_( edge.id ) ) );
            props.add( property_( PolyString.of( "__sourceId__" ), string_( edge.source ) ) );
            props.add( property_( PolyString.of( "__targetId__" ), string_( edge.target ) ) );
        }
        PolyString defIdentifier = edge.getVariableName();

        return edge_( defIdentifier, PolyString.of( edge.getRangeDescriptor() ), edge.labels, list_( props, "{", "}" ), edge.direction );
    }

    class PathStatement extends NeoStatement {

        private final List<ElementStatement> pathElements;
        private final PolyString identifier;


        protected PathStatement( PolyString identifier, List<ElementStatement> pathElements ) {
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
                            paths.add( node_( (String) null ) );
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
            String namedPath = identifier == null || identifier.value.contains( "$" ) ? "" : String.format( "%s = ", identifier );
            return namedPath + pathElements.stream().map( NeoStatement::build ).collect( Collectors.joining() );
        }

    }

    static PathStatement path_( ElementStatement... elements ) {
        return new PathStatement( null, Arrays.asList( elements ) );
    }

    static PathStatement path_( List<ElementStatement> elements ) {
        return new PathStatement( null, elements );
    }


    static PathStatement path_( @Nullable PolyString identifier, PolyPath path, @Nullable PolyString mappingLabel, boolean addId ) {
        int i = 0;
        List<ElementStatement> elements = new ArrayList<>();
        for ( GraphObject object : path.getPath() ) {
            elements.add( i % 2 == 0
                    ? node_( object.asNode(), mappingLabel, addId )
                    : edge_( object.asEdge(), addId ) );
            i++;
        }

        PolyString name = path.getVariableName() == null ? identifier : path.getVariableName();

        return new PathStatement( name, elements );
    }


    class PropertyStatement extends NeoStatement {

        private final PolyString key;
        private final NeoStatement value;


        protected PropertyStatement( PolyString key, NeoStatement value ) {
            super( null );
            this.key = key;
            this.value = value;
        }


        @Override
        public String build() {
            return key + ":" + value.build();
        }


    }

    static PropertyStatement property_( PolyString key, NeoStatement value ) {
        return new PropertyStatement( key, value );
    }

    static PropertyStatement property_( String key, NeoStatement value ) {
        return new PropertyStatement( PolyString.of( key ), value );
    }

    static List<PropertyStatement> identityProperties_( List<? extends PhysicalField> fields ) {
        List<PropertyStatement> props = new ArrayList<>();
        for ( PhysicalField field : fields ) {
            props.add( property_( PolyString.of( field.name ), identifier_( field.logicalName ) ) );
        }
        return props;
    }

    static List<PropertyStatement> properties_( PolyDictionary properties ) {
        List<PropertyStatement> props = new ArrayList<>();
        for ( Entry<PolyString, PolyValue> entry : properties.entrySet() ) {
            props.add( property_( entry.getKey(), _literalOrString( entry.getValue() ) ) );
        }
        return props;
    }

    static NeoStatement _literalOrString( PolyValue value ) {
        if ( value.isString() ) {
            return string_( value );
        } else if ( value.isList() ) {
            return literal_( PolyString.of( String.format( "[%s]", value.asList().stream().map( value1 -> _literalOrString( (PolyValue) value1 ).build() ).collect( Collectors.joining( ", " ) ) ) ) );
        } else {
            return literal_( value );
        }
    }

    static NeoStatement identifier_( String identifier ) {
        return new LiteralStatement( identifier );
    }

    class LabelsStatement extends NeoStatement {

        private final List<PolyString> labels;


        protected LabelsStatement( List<PolyString> labels ) {
            super( null );
            this.labels = labels;
        }


        @Override
        public String build() {
            return labels.stream().map( l -> ":" + l ).collect( Collectors.joining() );
        }


    }

    static LabelsStatement labels_( PolyString... labels ) {
        return new LabelsStatement( Arrays.asList( labels ) );
    }

    static LabelsStatement labels_( List<PolyString> labels ) {
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

    static LiteralStatement literal_( PolyValue value ) {
        return new LiteralStatement( value == null ? null : value.toString() );
    }

    static LiteralStatement string_( PolyValue value ) {
        return new LiteralStatement( value == null || value.isNull() ? null : "'" + value + "'" );
    }

    static LiteralStatement literal_( RexLiteral literal ) {
        String prePostFix = "";
        if ( PolyTypeFamily.CHARACTER.contains( literal.getType() ) ) {
            prePostFix = "\"";
        }
        return literal_( PolyString.of( String.format( "%s%s%s",
                prePostFix,
                NeoUtil.rexAsString( literal, null, false ),
                prePostFix ) ) );

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

    class ForeachStatement extends OperatorStatement {

        private final String elementId;
        private final NeoStatement collection;
        private final List<NeoStatement> statements;


        protected ForeachStatement( String elementId, NeoStatement collection, List<NeoStatement> statements ) {
            super( StatementType.FOREACH, list_( statements ) );

            this.elementId = elementId;
            this.collection = collection;
            this.statements = statements;
        }


        @Override
        public String build() {
            return String.format( "%s (%s IN %s | %s )", StatementType.FOREACH.toString(), elementId, collection.build(), statements.stream().map( NeoStatement::build ).collect( Collectors.joining( "" ) ) );
        }

    }

    static ForeachStatement foreach_( String elementId, NeoStatement collection, NeoStatement... statement ) {
        return new ForeachStatement( elementId, collection, Arrays.asList( statement ) );
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

    static LimitStatement limit_( PolyNumber limit ) {
        return new LimitStatement( literal_( limit ) );
    }

    class SkipStatement extends OperatorStatement {

        protected SkipStatement( NeoStatement statement ) {
            super( StatementType.SKIP, list_( List.of( statement ) ) );
        }

    }

    static SkipStatement skip_( PolyNumber offset ) {
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

    class AggregateStatement extends NeoStatement {

        private final String wrapper;
        private final String identifier;


        protected AggregateStatement( String wrapper, String identifier ) {
            super( StatementType.AGGREGATE );
            this.wrapper = wrapper;
            this.identifier = identifier;
        }


        @Override
        public String build() {
            return String.format( "%s(%s)", wrapper, identifier );
        }

    }

    static AggregateStatement count_( String identifier ) {
        return new AggregateStatement( "COUNT", identifier );
    }

}
