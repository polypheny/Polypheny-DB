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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.calcite.linq4j.function.Function1;
import org.polypheny.db.adapter.neo4j.NeoRelationalImplementor;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.algebra.type.AlgDataTypeField;
import org.polypheny.db.nodes.BinaryOperator;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexCorrelVariable;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexFieldAccess;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexLocalRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexVisitorImpl;
import org.polypheny.db.type.PathType;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.util.Pair;

public class Translator extends RexVisitorImpl<String> {


    private final List<AlgDataTypeField> afterFields;
    private final Map<String, String> mapping;
    private final NeoRelationalImplementor implementor;
    private final List<AlgDataTypeField> beforeFields;
    private final String mappingLabel;
    private final boolean useBrackets;

    private static final List<OperatorName> binaries = Arrays.stream( OperatorName.values() )
            .filter( o -> o.getClazz() == BinaryOperator.class )
            .toList();


    public Translator(
            AlgDataType afterRowType,
            AlgDataType beforeRowType,
            Map<String, String> mapping,
            NeoRelationalImplementor implementor,
            @Nullable String mappingLabel,
            boolean useBrackets ) {
        super( true );
        this.afterFields = afterRowType.getFields();
        this.beforeFields = beforeRowType.getFields();
        this.mapping = mapping;
        this.implementor = implementor;
        this.mappingLabel = mappingLabel;
        this.useBrackets = useBrackets;
    }


    @Override
    public String visitLiteral( RexLiteral literal ) {
        return NeoUtil.rexAsString( literal, mappingLabel, true );
    }


    @Override
    public String visitIndexRef( RexIndexRef inputRef ) {
        String name = beforeFields.get( inputRef.getIndex() ).getName();
        if ( mapping.containsKey( name ) ) {
            return mapping.get( name );
        }
        name = adjustGraph( inputRef.getType(), name );
        return name;
    }


    private String adjustGraph( AlgDataType type, String name ) {
        if ( type instanceof PathType && (name == null || name.startsWith( "$" ) && name.endsWith( "$" )) ) {
            return "*";
        }
        if ( !useBrackets ) {
            return name;
        }
        return switch ( type.getPolyType() ) {
            case NODE -> String.format( "(%s)", name );
            case EDGE -> String.format( "-[%s]-", name );
            default -> name;
        };
    }


    @Override
    public String visitLocalRef( RexLocalRef localRef ) {
        String name = afterFields.get( localRef.getIndex() ).getName();
        if ( mapping.containsKey( name ) ) {
            return mapping.get( name );
        }
        return name;
    }


    @Override
    public String visitCorrelVariable( RexCorrelVariable correlVariable ) {
        String name = afterFields.get( correlVariable.id.getId() ).getName();
        if ( mapping.containsKey( name ) ) {
            return mapping.get( name );
        }
        PolyType type = afterFields.get( correlVariable.id.getId() ).getType().getPolyType();
        if ( type == PolyType.NODE ) {
            name = String.format( "(%s)", name );
        }

        return name;
    }


    @Override
    public String visitDynamicParam( RexDynamicParam dynamicParam ) {
        if ( implementor != null ) {
            implementor.addPreparedType( dynamicParam );
            return NeoUtil.asParameter( dynamicParam.getIndex(), true );
        }
        throw new UnsupportedOperationException( "Prepared parameter is not possible without a implementor." );
    }


    @Override
    public String visitCall( RexCall call ) {
        if ( call.op.getOperatorName() == OperatorName.CYPHER_SET_LABELS ) {
            return handleSetLabels( call );
        }
        if ( call.op.getOperatorName() == OperatorName.CYPHER_SET_PROPERTIES ) {
            return handleSetProperties( call );
        }
        if ( call.op.getOperatorName() == OperatorName.CYPHER_EXTRACT_FROM_PATH ) {
            return handleExtractFromPath( call );
        }
        if ( call.op.getOperatorName() == OperatorName.CYPHER_SET_PROPERTY ) {
            return handleSetProperty( call );
        }
        if ( binaries.contains( call.op.getOperatorName() ) ) {
            return handleBinaries( call );
        }

        List<String> ops = call.operands.stream().map( o -> o.accept( this ) ).toList();

        return getFinalFunction( call, ops );
    }


    private String getFinalFunction( RexCall call, List<String> ops ) {
        Function1<List<String>, String> getter = NeoUtil.getOpAsNeo( call.op.getOperatorName(), call.operands, call.type );
        assert getter != null : "Function is not supported by the Neo4j adapter.";
        if ( useBrackets ) {
            return "(" + getter.apply( ops ) + ")";
        }
        return " " + getter.apply( ops ) + " ";
    }


    @Override
    public String visitFieldAccess( RexFieldAccess fieldAccess ) {
        if ( mapping != null ) {
            return mapping.get( fieldAccess.getField().getName() );
        }
        return fieldAccess.getField().getName();
    }


    private String handleBinaries( RexCall call ) {
        RexNode leftRex = call.operands.get( 0 );
        RexNode rightRex = call.operands.get( 1 );
        String left = leftRex.accept( this );
        if ( leftRex.isA( Kind.LITERAL ) && PolyType.STRING_TYPES.contains( leftRex.getType().getPolyType() ) ) {
            left = String.format( "'%s'", left );
        }
        String right = rightRex.accept( this );
        if ( rightRex.isA( Kind.LITERAL ) && PolyType.STRING_TYPES.contains( rightRex.getType().getPolyType() ) ) {
            right = String.format( "'%s'", right );
        }

        return getFinalFunction( call, List.of( left, right ) );

    }


    private String handleExtractFromPath( RexCall call ) {
        //AlgDataTypeField field = beforeFields.get( ((RexInputRef) call.operands.get( 0 )).getIndex() );
        assert call.operands.get( 1 ).isA( Kind.LITERAL );

        return ((RexLiteral) call.operands.get( 1 )).value.asString().value;
    }


    private String handleSetProperties( RexCall call ) {
        String identifier = call.operands.get( 0 ).accept( this );
        List<PolyValue> rexKeys = ((RexLiteral) call.operands.get( 1 )).value.asList();
        List<String> keys = rexKeys.stream().map( l -> l.asString().value ).toList();
        List<PolyValue> rexValues = ((RexLiteral) call.operands.get( 2 )).value.asList().value;
        List<String> values = rexValues.stream().map( l -> {
            String literal = l.toJson();
            if ( l.isString() ) {
                return String.format( "'%s'", literal );
            }
            return literal;
        } ).toList();

        boolean replace = ((RexLiteral) call.operands.get( 3 )).value.asBoolean().value;

        List<String> mappings = new ArrayList<>();
        mappings.add( String.format( "_id:%s._id", identifier ) );
        if ( call.getType().getPolyType() == PolyType.EDGE ) {
            mappings.add( String.format( "__sourceId__:%s.__sourceId__", identifier ) );
            mappings.add( String.format( "__targetId__:%s.__targetId__", identifier ) );
        }

        return identifier + (replace ? " = " : " += ") + "{" + Stream.concat(
                        Pair.zip( keys, values )
                                .stream()
                                .map( e -> String.format( "%s:%s", e.left, e.right ) ),
                        mappings.stream() ) // preserve id
                .collect( Collectors.joining( "," ) ) + "}";
    }


    private String handleSetProperty( RexCall call ) {
        String identifier = call.operands.get( 0 ).accept( this );
        String key = call.operands.get( 1 ).unwrap( RexLiteral.class ).orElseThrow().value.asString().value;
        RexLiteral rex = (RexLiteral) call.operands.get( 2 );
        String literal = NeoUtil.rexAsString( rex, null, true );
        if ( PolyType.STRING_TYPES.contains( rex.getType().getPolyType() ) ) {
            literal = String.format( "'%s'", rex.getValue() );
        }
        return String.format( "%s.%s = %s", identifier, key, literal );
    }


    private String handleSetLabels( RexCall call ) {
        String identifier = call.operands.get( 0 ).accept( this );
        List<PolyValue> rexLabels = ((RexLiteral) call.operands.get( 1 )).value.asList();
        List<String> labels = rexLabels.stream().map( l -> ":" + l.asString().value ).toList();
        return identifier + String.join( "", labels );
    }

}
