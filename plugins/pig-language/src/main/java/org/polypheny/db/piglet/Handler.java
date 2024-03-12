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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.polypheny.db.algebra.AlgNode;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeFactory;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.piglet.Ast.Direction;
import org.polypheny.db.piglet.Ast.PigNode;
import org.polypheny.db.rex.RexBuilder;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.tools.AlgBuilder;
import org.polypheny.db.tools.PigAlgBuilder;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Pair;


/**
 * Walks over a Pig AST and calls the corresponding methods in a {@link PigAlgBuilder}.
 */
public class Handler {

    private final PigAlgBuilder builder;
    private final Map<String, AlgNode> map = new HashMap<>();


    public Handler( PigAlgBuilder builder ) {
        this.builder = builder;
    }


    /**
     * Creates relational expressions for a given AST pigNode.
     */
    public Handler handle( PigNode pigNode ) {
        final AlgNode input;
        final List<RexNode> rexNodes;
        switch ( pigNode.op ) {
            case LOAD:
                final Ast.LoadStmt load = (Ast.LoadStmt) pigNode;
                builder.relScan( (String) load.name.value );
                register( load.target.value );
                return this;

            case VALUES:
                final Ast.ValuesStmt values = (Ast.ValuesStmt) pigNode;
                final AlgDataType rowType = toType( values.schema );
                builder.values( tuples( values, rowType ), rowType );
                register( values.target.value );
                return this;

            case FOREACH:
                final Ast.ForeachStmt foreach = (Ast.ForeachStmt) pigNode;
                builder.clear();
                input = map.get( foreach.source.value );
                builder.push( input );
                rexNodes = new ArrayList<>();
                for ( PigNode exp : foreach.expList ) {
                    rexNodes.add( toRex( exp ) );
                }
                builder.project( rexNodes );
                register( foreach.target.value );
                return this;

            case FOREACH_NESTED:
                final Ast.ForeachNestedStmt foreachNested = (Ast.ForeachNestedStmt) pigNode;
                builder.clear();
                input = map.get( foreachNested.source.value );
                builder.push( input );
                System.out.println( input.getTupleType() );
                for ( AlgDataTypeField field : input.getTupleType().getFields() ) {
                    if ( Objects.requireNonNull( field.getType().getPolyType() ) == PolyType.ARRAY ) {
                        System.out.println( field );
                    }
                }
                for ( Ast.Stmt stmt : foreachNested.nestedStmtList ) {
                    handle( stmt );
                }
                rexNodes = new ArrayList<>();
                for ( PigNode exp : foreachNested.expList ) {
                    rexNodes.add( toRex( exp ) );
                }
                builder.project( rexNodes );
                register( foreachNested.target.value );
                return this;

            case FILTER:
                final Ast.FilterStmt filter = (Ast.FilterStmt) pigNode;
                builder.clear();
                input = map.get( filter.source.value );
                builder.push( input );
                final RexNode rexNode = toRex( filter.condition );
                builder.filter( rexNode );
                register( filter.target.value );
                return this;

            case DISTINCT:
                final Ast.DistinctStmt distinct = (Ast.DistinctStmt) pigNode;
                builder.clear();
                input = map.get( distinct.source.value );
                builder.push( input );
                builder.distinct( null, -1 );
                register( distinct.target.value );
                return this;

            case ORDER:
                final Ast.OrderStmt order = (Ast.OrderStmt) pigNode;
                builder.clear();
                input = map.get( order.source.value );
                builder.push( input );
                final List<RexNode> nodes = new ArrayList<>();
                for ( Pair<Ast.Identifier, Ast.Direction> field : order.fields ) {
                    toSortRex( nodes, field );
                }
                builder.sort( nodes );
                register( order.target.value );
                return this;

            case LIMIT:
                final Ast.LimitStmt limit = (Ast.LimitStmt) pigNode;
                builder.clear();
                input = map.get( limit.source.value );
                final int count = ((Number) limit.count.value).intValue();
                builder.push( input );
                builder.limit( 0, count );
                register( limit.target.value );
                return this;

            case GROUP:
                final Ast.GroupStmt group = (Ast.GroupStmt) pigNode;
                builder.clear();
                input = map.get( group.source.value );
                builder.push( input ).as( group.source.value );
                final List<AlgBuilder.GroupKey> groupKeys = new ArrayList<>();
                final List<RexNode> keys = new ArrayList<>();
                if ( group.keys != null ) {
                    for ( PigNode key : group.keys ) {
                        keys.add( toRex( key ) );
                    }
                }
                groupKeys.add( builder.groupKey( keys ) );
                builder.group( PigAlgBuilder.GroupOption.COLLECTED, null, -1, groupKeys );
                register( group.target.value );
                return this;

            case PROGRAM:
                final Ast.Program program = (Ast.Program) pigNode;
                for ( Ast.Stmt stmt : program.stmtList ) {
                    handle( stmt );
                }
                return this;

            case DUMP:
                final Ast.DumpStmt dump = (Ast.DumpStmt) pigNode;
                final AlgNode algNode = map.get( dump.relation.value );
                dump( algNode );
                return this; // nothing to do; contains no algebra

            default:
                throw new AssertionError( "unknown operation " + pigNode.op );
        }
    }


    /**
     * Executes an algebra expression and prints the output.
     *
     * The default implementation does nothing.
     *
     * @param alg Relational expression
     */
    protected void dump( AlgNode alg ) {
    }


    private ImmutableList<ImmutableList<RexLiteral>> tuples( Ast.ValuesStmt valuesStmt, AlgDataType rowType ) {
        final ImmutableList.Builder<ImmutableList<RexLiteral>> listBuilder = ImmutableList.builder();
        for ( List<PigNode> pigNodeList : valuesStmt.tupleList ) {
            listBuilder.add( tuple( pigNodeList, rowType ) );
        }
        return listBuilder.build();
    }


    private ImmutableList<RexLiteral> tuple( List<PigNode> pigNodeList, AlgDataType rowType ) {
        final ImmutableList.Builder<RexLiteral> listBuilder = ImmutableList.builder();
        for ( Pair<PigNode, AlgDataTypeField> pair : Pair.zip( pigNodeList, rowType.getFields() ) ) {
            final PigNode pigNode = pair.left;
            final AlgDataType type = pair.right.getType();
            listBuilder.add( item( pigNode, type ) );
        }
        return listBuilder.build();
    }


    private ImmutableList<RexLiteral> bag( List<PigNode> pigNodeList, AlgDataType type ) {
        final ImmutableList.Builder<RexLiteral> listBuilder = ImmutableList.builder();
        for ( PigNode pigNode : pigNodeList ) {
            listBuilder.add( item( pigNode, type.getComponentType() ) );
        }
        return listBuilder.build();
    }


    private RexLiteral item( PigNode pigNode, AlgDataType type ) {
        final RexBuilder rexBuilder = builder.getRexBuilder();
        switch ( pigNode.op ) {
            case LITERAL:
                final Ast.Literal literal = (Ast.Literal) pigNode;
                return (RexLiteral) rexBuilder.makeLiteral( literal.value, type, false );

            case TUPLE:
                final Ast.Call tuple = (Ast.Call) pigNode;
                final ImmutableList<RexLiteral> list = tuple( tuple.operands, type );
                return (RexLiteral) rexBuilder.makeLiteral( list, type, false );

            case BAG:
                final Ast.Call bag = (Ast.Call) pigNode;
                final ImmutableList<RexLiteral> list2 = bag( bag.operands, type );
                return (RexLiteral) rexBuilder.makeLiteral( list2, type, false );

            default:
                throw new IllegalArgumentException( "not a literal: " + pigNode );
        }
    }


    private AlgDataType toType( Ast.Schema schema ) {
        final AlgDataTypeFactory.Builder typeBuilder = builder.getTypeFactory().builder();
        for ( Ast.FieldSchema fieldSchema : schema.fieldSchemaList ) {
            typeBuilder.add( null, fieldSchema.id.value, null, toType( fieldSchema.type ) );
        }
        return typeBuilder.build();
    }


    private AlgDataType toType( Ast.Type type ) {
        switch ( type.op ) {
            case SCALAR_TYPE:
                return toType( (Ast.ScalarType) type );
            case BAG_TYPE:
                return toType( (Ast.BagType) type );
            case MAP_TYPE:
                return toType( (Ast.MapType) type );
            case TUPLE_TYPE:
                return toType( (Ast.TupleType) type );
            default:
                throw new AssertionError( "unknown type " + type );
        }
    }


    private AlgDataType toType( Ast.ScalarType type ) {
        final AlgDataTypeFactory typeFactory = builder.getTypeFactory();
        return switch ( type.name ) {
            case "boolean" -> typeFactory.createPolyType( PolyType.BOOLEAN );
            case "int" -> typeFactory.createPolyType( PolyType.INTEGER );
            case "float" -> typeFactory.createPolyType( PolyType.REAL );
            default -> typeFactory.createPolyType( PolyType.VARCHAR );
        };
    }


    private AlgDataType toType( Ast.BagType type ) {
        final AlgDataTypeFactory typeFactory = builder.getTypeFactory();
        final AlgDataType t = toType( type.componentType );
        return typeFactory.createMultisetType( t, -1 );
    }


    private AlgDataType toType( Ast.MapType type ) {
        final AlgDataTypeFactory typeFactory = builder.getTypeFactory();
        final AlgDataType k = toType( type.keyType );
        final AlgDataType v = toType( type.valueType );
        return typeFactory.createMapType( k, v );
    }


    private AlgDataType toType( Ast.TupleType type ) {
        final AlgDataTypeFactory typeFactory = builder.getTypeFactory();
        final AlgDataTypeFactory.Builder builder = typeFactory.builder();
        for ( Ast.FieldSchema fieldSchema : type.fieldSchemaList ) {
            builder.add( null, fieldSchema.id.value, null, toType( fieldSchema.type ) );
        }
        return builder.build();
    }


    private void toSortRex(
            List<RexNode> nodes,
            Pair<Ast.Identifier, Ast.Direction> pair ) {
        if ( pair.left.isStar() ) {
            for ( RexNode node : builder.fields() ) {
                if ( Objects.requireNonNull( pair.right ) == Direction.DESC ) {
                    node = builder.desc( node );
                }
                nodes.add( node );
            }
        } else {
            RexNode node = toRex( pair.left );
            if ( Objects.requireNonNull( pair.right ) == Direction.DESC ) {
                node = builder.desc( node );
            }
            nodes.add( node );
        }
    }


    private RexNode toRex( PigNode exp ) {
        final Ast.Call call;
        switch ( exp.op ) {
            case LITERAL:
                return builder.literal( ((Ast.Literal) exp).value );
            case IDENTIFIER:
                final String value = ((Ast.Identifier) exp).value;
                if ( value.matches( "^\\$[0-9]+" ) ) {
                    int i = Integer.valueOf( value.substring( 1 ) );
                    return builder.field( i );
                }
                return builder.field( value );
            case DOT:
                call = (Ast.Call) exp;
                final RexNode left = toRex( call.operands.get( 0 ) );
                final Ast.Identifier right = (Ast.Identifier) call.operands.get( 1 );
                return builder.dot( left, right.value );
            case EQ:
            case NE:
            case GT:
            case GTE:
            case LT:
            case LTE:
            case AND:
            case OR:
            case NOT:
            case PLUS:
            case MINUS:
                call = (Ast.Call) exp;
                return builder.call( op( exp.op ), toRex( call.operands ) );
            default:
                throw new AssertionError( "unknown op " + exp.op );
        }
    }


    private static Operator op( Ast.Op op ) {
        return switch ( op ) {
            case EQ -> OperatorRegistry.get( OperatorName.EQUALS );
            case NE -> OperatorRegistry.get( OperatorName.NOT_EQUALS );
            case GT -> OperatorRegistry.get( OperatorName.GREATER_THAN );
            case GTE -> OperatorRegistry.get( OperatorName.GREATER_THAN_OR_EQUAL );
            case LT -> OperatorRegistry.get( OperatorName.LESS_THAN );
            case LTE -> OperatorRegistry.get( OperatorName.LESS_THAN_OR_EQUAL );
            case AND -> OperatorRegistry.get( OperatorName.AND );
            case OR -> OperatorRegistry.get( OperatorName.OR );
            case NOT -> OperatorRegistry.get( OperatorName.NOT );
            case PLUS -> OperatorRegistry.get( OperatorName.PLUS );
            case MINUS -> OperatorRegistry.get( OperatorName.MINUS );
            default -> throw new AssertionError( "unknown: " + op );
        };
    }


    private ImmutableList<RexNode> toRex( Iterable<PigNode> operands ) {
        final ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
        for ( PigNode operand : operands ) {
            builder.add( toRex( operand ) );
        }
        return builder.build();
    }


    /**
     * Assigns the current relational expression to a given name.
     */
    private void register( String name ) {
        map.put( name, builder.peek() );
    }

}

