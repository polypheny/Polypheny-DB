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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.piglet;


import com.google.common.collect.ImmutableList;
import java.math.BigDecimal;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import lombok.Getter;
import org.apache.calcite.avatica.util.Spacer;
import org.apache.calcite.linq4j.Ord;
import org.jetbrains.annotations.Nullable;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.nodes.Node;
import org.polypheny.db.nodes.NodeVisitor;
import org.polypheny.db.util.CoreUtil;
import org.polypheny.db.util.Litmus;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.Util;


/**
 * Abstract syntax tree.
 *
 * Contains inner classes for various kinds of parse tree node.
 */
public class Ast {

    private Ast() {
    }


    public static String toString( PigNode x ) {
        return new UnParser().append( x ).buf.toString();
    }


    /**
     * Formats a node and its children as a string.
     */
    public static UnParser unParse( UnParser u, PigNode n ) {
        switch ( n.op ) {
            case PROGRAM:
                final Program program = (Program) n;
                return u.append( "{op: PROGRAM, stmts: " ).appendList( program.stmtList ).append( "}" );
            case LOAD:
                final LoadStmt load = (LoadStmt) n;
                return u.append( "{op: LOAD, target: " + load.target.value + ", name: " + load.name.value + "}" );
            case DUMP:
                final DumpStmt dump = (DumpStmt) n;
                return u.append( "{op: DUMP, relation: " + dump.relation.value + "}" );
            case DESCRIBE:
                final DescribeStmt describe = (DescribeStmt) n;
                return u.append( "{op: DESCRIBE, relation: " + describe.relation.value + "}" );
            case FOREACH:
                final ForeachStmt foreach = (ForeachStmt) n;
                return u.append( "{op: FOREACH, target: " + foreach.target.value + ", source: " + foreach.source.value + ", expList: " )
                        .appendList( foreach.expList )
                        .append( "}" );
            case FOREACH_NESTED:
                final ForeachNestedStmt foreachNested = (ForeachNestedStmt) n;
                return u.append( "{op: FOREACH, target: " + foreachNested.target.value + ", source: " + foreachNested.source.value + ", nestedOps: " )
                        .appendList( foreachNested.nestedStmtList )
                        .append( ", expList: " )
                        .appendList( foreachNested.expList )
                        .append( "}" );
            case FILTER:
                final FilterStmt filter = (FilterStmt) n;
                u.append( "{op: FILTER, target: " + filter.target.value + ", source: " + filter.source.value + ", condition: " );
                u.in().append( filter.condition ).out();
                return u.append( "}" );
            case DISTINCT:
                final DistinctStmt distinct = (DistinctStmt) n;
                return u.append( "{op: DISTINCT, target: " + distinct.target.value + ", source: " + distinct.source.value + "}" );
            case LIMIT:
                final LimitStmt limit = (LimitStmt) n;
                return u.append( "{op: LIMIT, target: " ).append( limit.target.value )
                        .append( ", source: " ).append( limit.source.value )
                        .append( ", count: " ).append( limit.count.value.toString() )
                        .append( "}" );
            case ORDER:
                final OrderStmt order = (OrderStmt) n;
                return u.append( "{op: ORDER, target: " + order.target.value + ", source: " + order.source.value + "}" );
            case GROUP:
                final GroupStmt group = (GroupStmt) n;
                u.append( "{op: GROUP, target: " + group.target.value + ", source: " + group.source.value );
                if ( group.keys != null ) {
                    u.append( ", keys: " ).appendList( group.keys );
                }
                return u.append( "}" );
            case LITERAL:
                final Literal literal = (Literal) n;
                return u.append( String.valueOf( literal.value ) );
            case IDENTIFIER:
                final Identifier id = (Identifier) n;
                return u.append( id.value );
            default:
                throw new AssertionError( "unknown op " + n.op );
        }
    }


    /**
     * Parse tree node type.
     */
    public enum Op {
        PROGRAM,

        // atoms
        LITERAL, IDENTIFIER, BAG, TUPLE,

        // statements
        DESCRIBE, DISTINCT, DUMP, LOAD, FOREACH, FILTER,
        FOREACH_NESTED, LIMIT, ORDER, GROUP, VALUES,

        // types
        SCHEMA, SCALAR_TYPE, BAG_TYPE, TUPLE_TYPE, MAP_TYPE, FIELD_SCHEMA,

        // operators
        DOT, EQ, NE, GT, LT, GTE, LTE, PLUS, MINUS, AND, OR, NOT
    }


    /**
     * Abstract base class for parse tree node.
     */
    public abstract static class PigNode implements Node {

        public final Op op;
        @Getter
        public final ParserPos pos;


        protected PigNode( ParserPos pos, Op op ) {
            this.op = Objects.requireNonNull( op );
            this.pos = Objects.requireNonNull( pos );
        }


        @Override
        public @Nullable String getEntity() {
            return null;
        }


        @Override
        public Node clone( ParserPos pos ) {
            throw new UnsupportedOperationException( "Pig nodes cannot be cloned." );
        }


        @Override
        public Kind getKind() {
            return Kind.OTHER;
        }


        @Override
        public QueryLanguage getLanguage() {
            return QueryLanguage.from( "pig" );
        }


        @Override
        public boolean isA( Set<Kind> category ) {
            throw new UnsupportedOperationException( "Pig does not support this operation." );
        }


        @Override
        public boolean equalsDeep( Node node, Litmus litmus ) {
            throw new UnsupportedOperationException( "Pig does not support this operation." );
        }


        @Override
        public <R> R accept( NodeVisitor<R> visitor ) {
            throw new UnsupportedOperationException( "Pig does not support this operation." );
        }

    }


    /**
     * Abstract base class for parse tree node representing a statement.
     */
    public abstract static class Stmt extends PigNode {

        protected Stmt( ParserPos pos, Op op ) {
            super( pos, op );
        }

    }


    /**
     * Abstract base class for statements that assign to a named relation.
     */
    public abstract static class Assignment extends Stmt {

        final Identifier target;


        protected Assignment( ParserPos pos, Op op, Identifier target ) {
            super( pos, op );
            this.target = Objects.requireNonNull( target );
        }

    }


    /**
     * Parse tree node for LOAD statement.
     */
    public static class LoadStmt extends Assignment {

        final Literal name;


        public LoadStmt( ParserPos pos, Identifier target, Literal name ) {
            super( pos, Op.LOAD, target );
            this.name = Objects.requireNonNull( name );
        }


        @Override
        public @Nullable String getEntity() {
            return name.value.toString();
        }

    }


    /**
     * Parse tree node for VALUES statement.
     *
     * VALUES is an extension to Pig, inspired by SQL's VALUES clause.
     */
    public static class ValuesStmt extends Assignment {

        final List<List<PigNode>> tupleList;
        final Schema schema;


        public ValuesStmt( ParserPos pos, Identifier target, Schema schema, List<List<PigNode>> tupleList ) {
            super( pos, Op.VALUES, target );
            this.schema = schema;
            this.tupleList = ImmutableList.copyOf( tupleList );
        }

    }


    /**
     * Abstract base class for an assignment with one source relation.
     */
    public static class Assignment1 extends Assignment {

        final Identifier source;


        protected Assignment1( ParserPos pos, Op op, Identifier target, Identifier source ) {
            super( pos, op, target );
            this.source = source;
        }

    }


    /**
     * Parse tree node for FOREACH statement (non-nested).
     *
     * Syntax:
     * <code>
     * alias = FOREACH alias GENERATE expression [, expression]... [ AS schema ];
     * </code>
     *
     * @see org.polypheny.db.piglet.Ast.ForeachNestedStmt
     */
    public static class ForeachStmt extends Assignment1 {

        final List<PigNode> expList;


        public ForeachStmt( ParserPos pos, Identifier target, Identifier source, List<PigNode> expList, Schema schema ) {
            super( pos, Op.FOREACH, target, source );
            this.expList = expList;
            assert schema == null; // not supported yet
        }

    }


    /**
     * Parse tree node for FOREACH statement (nested).
     *
     * Syntax:
     *
     * <code>
     * alias = FOREACH nested_alias {
     * alias = nested_op; [alias = nested_op; ]...
     * GENERATE expression [, expression]...
     * };<br>
     * &nbsp;
     * nested_op ::= DISTINCT, FILTER, LIMIT, ORDER, SAMPLE
     * </code>
     *
     * @see ForeachStmt
     */
    public static class ForeachNestedStmt extends Assignment1 {

        final List<Stmt> nestedStmtList;
        final List<PigNode> expList;


        public ForeachNestedStmt( ParserPos pos, Identifier target, Identifier source, List<Stmt> nestedStmtList, List<PigNode> expList, Schema schema ) {
            super( pos, Op.FOREACH_NESTED, target, source );
            this.nestedStmtList = nestedStmtList;
            this.expList = expList;
            assert schema == null; // not supported yet
        }

    }


    /**
     * Parse tree node for FILTER statement.
     *
     * Syntax: <pre>alias = FILTER alias BY expression;</pre>
     */
    public static class FilterStmt extends Assignment1 {

        final PigNode condition;


        public FilterStmt( ParserPos pos, Identifier target, Identifier source, PigNode condition ) {
            super( pos, Op.FILTER, target, source );
            this.condition = condition;
        }

    }


    /**
     * Parse tree node for DISTINCT statement.
     *
     * Syntax: <pre>alias = DISTINCT alias;</pre>
     */
    public static class DistinctStmt extends Assignment1 {

        public DistinctStmt( ParserPos pos, Identifier target, Identifier source ) {
            super( pos, Op.DISTINCT, target, source );
        }

    }


    /**
     * Parse tree node for LIMIT statement.
     *
     * Syntax: <pre>alias = LIMIT alias n;</pre>
     */
    public static class LimitStmt extends Assignment1 {

        final Literal count;


        public LimitStmt(
                ParserPos pos, Identifier target,
                Identifier source, Literal count ) {
            super( pos, Op.LIMIT, target, source );
            this.count = count;
        }

    }


    /**
     * Parse tree node for ORDER statement.
     *
     * Syntax: <code>alias = ORDER alias BY (* | field) [ASC | DESC] [, field [ASC | DESC] ]...;</code>
     */
    public static class OrderStmt extends Assignment1 {

        final List<Pair<Identifier, Direction>> fields;


        public OrderStmt( ParserPos pos, Identifier target, Identifier source, List<Pair<Identifier, Direction>> fields ) {
            super( pos, Op.ORDER, target, source );
            this.fields = fields;
        }

    }


    /**
     * Parse tree node for GROUP statement.
     *
     * Syntax: <code>alias = GROUP alias ( ALL | BY ( exp | '(' exp [, exp]... ')' ) )</code>
     */
    public static class GroupStmt extends Assignment1 {

        /**
         * Grouping keys. May be null (for ALL), or a list of one or more expressions.
         */
        final List<PigNode> keys;


        public GroupStmt( ParserPos pos, Identifier target, Identifier source, List<PigNode> keys ) {
            super( pos, Op.GROUP, target, source );
            this.keys = keys;
            assert keys == null || keys.size() >= 1;
        }

    }


    /**
     * Parse tree node for DUMP statement.
     */
    public static class DumpStmt extends Stmt {

        final Identifier relation;


        public DumpStmt( ParserPos pos, Identifier relation ) {
            super( pos, Op.DUMP );
            this.relation = Objects.requireNonNull( relation );
        }

    }


    /**
     * Parse tree node for DESCRIBE statement.
     */
    public static class DescribeStmt extends Stmt {

        final Identifier relation;


        public DescribeStmt( ParserPos pos, Identifier relation ) {
            super( pos, Op.DESCRIBE );
            this.relation = Objects.requireNonNull( relation );
        }

    }


    /**
     * Parse tree node for Literal.
     */
    public static class Literal extends PigNode {

        final Object value;


        public Literal( ParserPos pos, Object value ) {
            super( pos, Op.LITERAL );
            this.value = Objects.requireNonNull( value );
        }


        public static NumericLiteral createExactNumeric( String s, ParserPos pos ) {
            BigDecimal value;
            int prec;
            int scale;

            int i = s.indexOf( '.' );
            if ( (i >= 0) && ((s.length() - 1) != i) ) {
                value = CoreUtil.parseDecimal( s );
                scale = s.length() - i - 1;
                assert scale == value.scale() : s;
                prec = s.length() - 1;
            } else if ( (i >= 0) && ((s.length() - 1) == i) ) {
                value = CoreUtil.parseInteger( s.substring( 0, i ) );
                scale = 0;
                prec = s.length() - 1;
            } else {
                value = CoreUtil.parseInteger( s );
                scale = 0;
                prec = s.length();
            }
            return new NumericLiteral( pos, value, prec, scale, true );
        }

    }


    /**
     * Parse tree node for NumericLiteral.
     */
    public static class NumericLiteral extends Literal {

        final int prec;
        final int scale;
        final boolean exact;


        NumericLiteral( ParserPos pos, BigDecimal value, int prec, int scale, boolean exact ) {
            super( pos, value );
            this.prec = prec;
            this.scale = scale;
            this.exact = exact;
        }


        public NumericLiteral negate( ParserPos pos ) {
            BigDecimal value = (BigDecimal) this.value;
            return new NumericLiteral( pos, value.negate(), prec, scale, exact );
        }

    }


    /**
     * Parse tree node for Identifier.
     */
    public static class Identifier extends PigNode {

        final String value;


        public Identifier( ParserPos pos, String value ) {
            super( pos, Op.IDENTIFIER );
            this.value = Objects.requireNonNull( value );
        }


        public boolean isStar() {
            return false;
        }

    }


    /**
     * Parse tree node for "*", a special kind of identifier.
     */
    public static class SpecialIdentifier extends Identifier {

        public SpecialIdentifier( ParserPos pos ) {
            super( pos, "*" );
        }


        @Override
        public boolean isStar() {
            return true;
        }

    }


    /**
     * Parse tree node for a call to a function or operator.
     */
    public static class Call extends PigNode {

        final ImmutableList<PigNode> operands;


        private Call( ParserPos pos, Op op, ImmutableList<PigNode> operands ) {
            super( pos, op );
            this.operands = ImmutableList.copyOf( operands );
        }


        public Call( ParserPos pos, Op op, Iterable<? extends PigNode> operands ) {
            this( pos, op, ImmutableList.copyOf( operands ) );
        }


        public Call( ParserPos pos, Op op, PigNode... operands ) {
            this( pos, op, ImmutableList.copyOf( operands ) );
        }

    }


    /**
     * Parse tree node for a program.
     */
    public static class Program extends PigNode {

        public final List<Stmt> stmtList;


        public Program( ParserPos pos, List<Stmt> stmtList ) {
            super( pos, Op.PROGRAM );
            this.stmtList = stmtList;
        }

    }


    /**
     * Parse tree for field schema.
     *
     * Syntax: <pre>identifier:type</pre>
     */
    public static class FieldSchema extends PigNode {

        final Identifier id;
        final Type type;


        public FieldSchema( ParserPos pos, Identifier id, Type type ) {
            super( pos, Op.FIELD_SCHEMA );
            this.id = Objects.requireNonNull( id );
            this.type = Objects.requireNonNull( type );
        }

    }


    /**
     * Parse tree for schema.
     *
     * Syntax: <pre>AS ( identifier:type [, identifier:type]... )</pre>
     */
    public static class Schema extends PigNode {

        final List<FieldSchema> fieldSchemaList;


        public Schema( ParserPos pos, List<FieldSchema> fieldSchemaList ) {
            super( pos, Op.SCHEMA );
            this.fieldSchemaList = ImmutableList.copyOf( fieldSchemaList );
        }

    }


    /**
     * Parse tree for type.
     */
    public abstract static class Type extends PigNode {

        protected Type( ParserPos pos, Op op ) {
            super( pos, op );
        }

    }


    /**
     * Parse tree for scalar type such as {@code int}.
     */
    public static class ScalarType extends Type {

        final String name;


        public ScalarType( ParserPos pos, String name ) {
            super( pos, Op.SCALAR_TYPE );
            this.name = name;
        }

    }


    /**
     * Parse tree for a bag type.
     */
    public static class BagType extends Type {

        final Type componentType;


        public BagType( ParserPos pos, Type componentType ) {
            super( pos, Op.BAG_TYPE );
            this.componentType = componentType;
        }

    }


    /**
     * Parse tree for a tuple type.
     */
    public static class TupleType extends Type {

        final List<FieldSchema> fieldSchemaList;


        public TupleType( ParserPos pos, List<FieldSchema> fieldSchemaList ) {
            super( pos, Op.TUPLE_TYPE );
            this.fieldSchemaList = ImmutableList.copyOf( fieldSchemaList );
        }

    }


    /**
     * Parse tree for a map type.
     */
    public static class MapType extends Type {

        final Type keyType;
        final Type valueType;


        public MapType( ParserPos pos ) {
            super( pos, Op.MAP_TYPE );
            // REVIEW: Why does Pig's "map" type not have key and value types?
            this.keyType = new ScalarType( pos, "int" );
            this.valueType = new ScalarType( pos, "int" );
        }

    }


    /**
     * Contains output and indentation level while a tree of nodes is being converted to text.
     */
    static class UnParser {

        final StringBuilder buf = new StringBuilder();
        final Spacer spacer = new Spacer( 0 );


        public UnParser in() {
            spacer.add( 2 );
            return this;
        }


        public UnParser out() {
            spacer.subtract( 2 );
            return this;
        }


        public UnParser newline() {
            buf.append( Util.LINE_SEPARATOR );
            spacer.spaces( buf );
            return this;
        }


        public UnParser append( String s ) {
            buf.append( s );
            return this;
        }


        public UnParser append( PigNode n ) {
            return unParse( this, n );
        }


        public UnParser appendList( List<? extends PigNode> list ) {
            append( "[" ).in();
            for ( Ord<PigNode> n : Ord.<PigNode>zip( list ) ) {
                newline().append( n.e );
                if ( n.i < list.size() - 1 ) {
                    append( "," );
                }
            }
            return out().append( "]" );
        }

    }


    /**
     * Sort direction.
     */
    public enum Direction {
        ASC,
        DESC,
        NOT_SPECIFIED
    }

}
