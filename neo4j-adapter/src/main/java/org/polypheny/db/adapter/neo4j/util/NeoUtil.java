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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.polypheny.db.adapter.enumerable.EnumUtils;
import org.polypheny.db.adapter.neo4j.NeoEntity;
import org.polypheny.db.adapter.neo4j.rules.NeoProject;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexInputRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;

public interface NeoUtil {

    static Function1<Value, Object> getTypeFunction( PolyType type, PolyType componentType ) {

        switch ( type ) {
            case BOOLEAN:
                return Value::asBoolean;
            case TINYINT:
            case SMALLINT:
            case INTEGER:
                return Value::asInt;
            case BIGINT:
                return Value::asLong;
            case DECIMAL:
            case FLOAT:
                return Value::asFloat;
            case REAL:
            case DOUBLE:
                return Value::asDouble;
            case DATE:
                break;
            case TIME:
                break;
            case TIME_WITH_LOCAL_TIME_ZONE:
                break;
            case TIMESTAMP:
                break;
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                break;
            case INTERVAL_YEAR:
                break;
            case INTERVAL_YEAR_MONTH:
                break;
            case INTERVAL_MONTH:
                break;
            case INTERVAL_DAY:
                break;
            case INTERVAL_DAY_HOUR:
                break;
            case INTERVAL_DAY_MINUTE:
                break;
            case INTERVAL_DAY_SECOND:
                break;
            case INTERVAL_HOUR:
                break;
            case INTERVAL_HOUR_MINUTE:
                break;
            case INTERVAL_HOUR_SECOND:
                break;
            case INTERVAL_MINUTE:
                break;
            case INTERVAL_MINUTE_SECOND:
                break;
            case INTERVAL_SECOND:
                break;
            case CHAR:
            case VARCHAR:
                return Value::asString;
            case BINARY:
                break;
            case VARBINARY:
                break;
            case NULL:
                break;
            case ANY:
                break;
            case SYMBOL:
                break;
            case MULTISET:
                break;
            case ARRAY:
                break;
            case MAP:
                break;
            case DOCUMENT:
                break;
            case GRAPH:
                break;
            case NODE:
                break;
            case EDGE:
                break;
            case PATH:
                break;
            case DISTINCT:
                break;
            case STRUCTURED:
                break;
            case ROW:
                break;
            case OTHER:
                break;
            case CURSOR:
                break;
            case COLUMN_LIST:
                break;
            case DYNAMIC_STAR:
                break;
            case GEOMETRY:
                break;
            case FILE:
                break;
            case IMAGE:
                break;
            case VIDEO:
                break;
            case SOUND:
                break;
            case JSON:
                break;
        }

        throw new RuntimeException( String.format( "Object of type %s was not transformable.", type ) );
    }

    static Function1<Record, Object> getTypesFunction( List<PolyType> types, List<PolyType> componentTypes ) {
        int i = 0;
        List<Function1<Value, Object>> functions = new ArrayList<>();
        for ( PolyType type : types ) {
            functions.add( getTypeFunction( type, componentTypes.get( i ) ) );
            i++;
        }
        if ( functions.size() == 1 ) {
            // SCALAR
            return o -> functions.get( 0 ).apply( o.get( 0 ) );
        }

        // ARRAY
        return o -> Pair.zip( o.fields(), functions ).stream().map( e -> e.right.apply( e.left.value() ) ).collect( Collectors.toList() ).toArray();
    }

    enum StatementType {
        MATCH( "MATCH" ),
        CREATE( "CREATE" ),
        WHERE( "WHERE" ),
        RETURN( "RETURN" ),
        WITH( "WITH" );

        public final String identifier;


        StatementType( String identifier ) {
            this.identifier = identifier;
        }
    }


    @Getter
    abstract class NeoStatement {

        final StatementType type;
        public final List<Pair<String, String>> tableCols;


        protected NeoStatement( StatementType type, List<Pair<String, String>> tableCols ) {
            this.type = type;
            this.tableCols = tableCols;
        }


        public static NeoStatement createReturn( List<RexNode> projects, NeoEntity entity, List<Pair<String, String>> tableCols ) {
            boolean isPrepared = false;
            List<String> chunks = new ArrayList<>();
            List<Long> indexes = new ArrayList<>();
            List<PolyType> types = new ArrayList<>();
            List<PolyType> componentTypes = new ArrayList<>();

            List<AlgDataTypeField> fields = entity.getRowType( entity.getTypeFactory() ).getFieldList();

            boolean unprojected = tableCols.size() == 1 && tableCols.get( 0 ).right == null;
            StringBuilder query = new StringBuilder();
            List<String> names = new ArrayList<>();
            int i = 0;
            String temp = "";
            for ( RexNode node : projects ) {
                if ( node.isA( Kind.INPUT_REF ) ) {
                    query.append( temp )
                            .append( unprojected ? tableCols.get( 0 ).left + "." + fields.get( ((RexInputRef) node).getIndex() ).getPhysicalName() : tableCols.get( 0 ).left )
                            .append( " AS " )
                            .append( fields.get( ((RexInputRef) node).getIndex() ).getName() );
                    names.add( fields.get( ((RexInputRef) node).getIndex() ).getName() );
                } else if ( node.isA( Kind.DYNAMIC_PARAM ) ) {
                    //chunks.add( temp.append(  ) );
                } else {
                    throw new UnsupportedOperationException( "This type is not supported." );
                }
                temp = ", ";
                i++;
            }
            if ( isPrepared ) {
                return new PreparedReturn( chunks, indexes, types, componentTypes );
            } else {
                return new ReturnStatement( query.toString(), names.stream().map( n -> Pair.of( (String) null, n ) ).collect( Collectors.toList() ) );
            }
        }


        public abstract boolean isPrepare();

        public abstract String build( Map<Long, AlgDataType> types, Map<Long, Object> values );


        public abstract Expression asExpression();

    }


    abstract class PreparedStatement extends NeoStatement {


        public final List<String> queryChunks;
        public final List<Long> insertIds;
        public final List<PolyType> types;
        private final List<PolyType> componentTypes;


        protected PreparedStatement( StatementType type, List<String> queryChunks, List<Long> insertIds, List<PolyType> types, List<PolyType> componentTypes, List<Pair<String, String>> tableCols ) {
            super( type, tableCols );
            this.queryChunks = queryChunks;
            this.insertIds = insertIds;
            this.types = types;
            this.componentTypes = componentTypes;
            assert this.insertIds.size() == this.types.size();
            assert this.queryChunks.size() == (this.insertIds.size() + 1);
        }


        @Override
        public boolean isPrepare() {
            return true;
        }


        @Override
        public Expression asExpression() {
            return Expressions.new_(
                    getClass(),
                    EnumUtils.constantArrayList( queryChunks, String.class ),
                    EnumUtils.constantArrayList( insertIds, Long.class ),
                    EnumUtils.constantArrayList( types, PolyType.class ),
                    EnumUtils.constantArrayList( componentTypes, PolyType.class ) );
        }


        @Override
        public String build( Map<Long, AlgDataType> types, Map<Long, Object> values ) {
            String query = type.identifier + " ";
            int i = 0;
            Iterator<Long> iter = insertIds.iterator();
            // attach first part of query
            StringBuilder queryBuilder = new StringBuilder( query );
            while ( iter.hasNext() ) {
                Long j = iter.next();
                queryBuilder
                        .append( queryChunks.get( i ) )
                        .append( asString( values.get( j ), types.get( j ) ) );
                i++;
            }
            // attach last part
            queryBuilder.append( queryChunks.get( i ) );
            return queryBuilder.toString();
        }


        private Object asString( Object o, AlgDataType type ) {
            return o.toString();
        }


    }


    @Getter
    abstract class NormalStatement extends NeoStatement {

        public final String query;


        protected NormalStatement( StatementType type, String query, List<Pair<String, String>> tableCols ) {
            super( type, tableCols );
            this.query = query;
        }


        @Override
        public String build( @Nullable Map<Long, AlgDataType> types, @Nullable Map<Long, Object> values ) {
            return build();
        }


        public String build() {
            return type.identifier + " " + query;
        }


        @Override
        public boolean isPrepare() {
            return false;
        }


        @Override
        public Expression asExpression() {
            return Expressions.new_(
                    getClass(),
                    Expressions.constant( query, String.class ),
                    Expressions.constant( null ) );
        }

    }


    class MatchStatement extends NormalStatement {

        public MatchStatement( String query, List<Pair<String, String>> tableCols ) {
            super( StatementType.MATCH, query, tableCols );
        }


    }


    class WhereStatement extends NormalStatement {

        public WhereStatement( String query, List<Pair<String, String>> tableCols ) {
            super( StatementType.WHERE, query, tableCols );
        }

    }


    class PreparedWhere extends PreparedStatement {

        public PreparedWhere( List<String> queryChunks, List<Long> insertIds, List<PolyType> types, List<PolyType> componentTypes ) {
            super( StatementType.WHERE, queryChunks, insertIds, types, componentTypes, List.of( Pair.of( null, "n" ) ) );
        }

    }


    class CreateStatement extends NormalStatement {

        public CreateStatement( String query, List<Pair<String, String>> tableCols ) {
            super( StatementType.CREATE, query, tableCols );
        }


        public static CreateStatement create( ImmutableList<ImmutableList<RexLiteral>> values, NeoEntity entity ) {
            int nodeI = 0;
            List<String> nodes = new ArrayList<>();
            AlgDataType rowType = entity.getRowType( entity.getTypeFactory() );
            for ( ImmutableList<RexLiteral> row : values ) {
                int i = 0;
                String node = String.format( "(n%s:%s {", nodeI, entity.phsicalEntityName );
                List<String> props = new ArrayList<>();
                for ( RexLiteral value : row ) {
                    props.add( rowType.getFieldList().get( i ).getPhysicalName() + ":" + rexAsString( value ) );
                    i++;
                }
                node += String.join( ", ", props );
                node += "})";
                nodes.add( node );
                nodeI++;
            }
            String inserts = String.join( ", ", nodes );
            List<Pair<String, String>> tableCols = new ArrayList<>();
            for ( int i = 0; i < nodes.size(); i++ ) {
                tableCols.add( Pair.of( "n" + i, null ) );
            }

            return new CreateStatement( inserts, tableCols );
        }

    }

    private static String rexAsString( RexLiteral value ) {
        return value.getValueForQueryParameterizer().toString();
    }

    class PreparedCreate extends PreparedStatement {

        public PreparedCreate( List<String> chunks, List<Long> indexes, List<PolyType> types, List<PolyType> componentTypes ) {
            super( StatementType.CREATE, chunks, indexes, types, componentTypes, List.of( Pair.of( null, "n" ) ) );
        }


        public static PreparedCreate createPrepared( NeoProject last, NeoEntity entity ) {
            List<String> chunks = new ArrayList<>();
            List<Long> indexes = new ArrayList<>();
            List<PolyType> types = new ArrayList<>();
            List<PolyType> componentTypes = new ArrayList<>();

            StringBuilder temp = new StringBuilder( String.format( "( n:%s {", entity.phsicalEntityName ) );
            List<AlgDataTypeField> fields = entity.getRowType( entity.getTypeFactory() ).getFieldList();

            int i = 0;
            for ( RexNode project : last.getProjects() ) {
                String property = temp + fields.get( i ).getPhysicalName() + ":";
                if ( project.isA( Kind.LITERAL ) ) {
                    temp.append( property ).append( rexAsString( (RexLiteral) project ) );
                } else if ( project.isA( Kind.DYNAMIC_PARAM ) ) {
                    chunks.add( property );
                    indexes.add( ((RexDynamicParam) project).getIndex() );
                    types.add( project.getType().getPolyType() );
                    componentTypes.add( NeoUtil.getComponentTypeOrParent( project.getType() ) );
                } else {
                    throw new UnsupportedOperationException( "This operation is not supported." );
                }
                temp = new StringBuilder( "," );
                i++;
            }
            chunks.add( "})" );

            return new PreparedCreate( chunks, indexes, types, componentTypes );
        }

    }

    static PolyType getComponentTypeOrParent( AlgDataType type ) {
        if ( type.getPolyType() == PolyType.ARRAY ) {
            return type.getComponentType().getPolyType();
        }
        return type.getPolyType();
    }


    class ReturnStatement extends NormalStatement {

        public ReturnStatement( String query, List<Pair<String, String>> tableCols ) {
            super( StatementType.RETURN, query, tableCols );
        }

    }


    class PreparedReturn extends PreparedStatement {

        public PreparedReturn( List<String> queryChunks, List<Long> insertIds, List<PolyType> types, List<PolyType> componentTypes ) {
            super( StatementType.RETURN, queryChunks, insertIds, types, componentTypes, List.of( Pair.of( null, "n" ) ) );
        }

    }

}
