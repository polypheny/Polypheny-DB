/*
 * Copyright 2019-2026 The Polypheny Project
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

import static org.polypheny.db.adapter.neo4j.util.NeoStatements.quoteIdentifier;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
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
import org.polypheny.db.rex.RexVisitorImpl;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.entity.PolyValue;

public class Translator extends RexVisitorImpl<String> {


    private static final List<OperatorName> binaries = Arrays.stream( OperatorName.values() )
            .filter( o -> o.getClazz() == BinaryOperator.class )
            .toList();
    private final List<AlgDataTypeField> afterFields;
    private final Map<String, String> mapping;
    private final NeoRelationalImplementor implementor;
    private final List<AlgDataTypeField> beforeFields;
    private final String mappingLabel;
    private final boolean useBrackets;


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
        return adjustGraph( inputRef.getType(), name );
    }


    private String adjustGraph( AlgDataType type, String name ) {
        String identifier = mappingLabel == null ? name : quoteIdentifier( name );
        if ( !useBrackets ) {
            return identifier;
        }
        return switch ( type.getPolyType() ) {
            case NODE -> String.format( "(%s)", identifier );
            case EDGE -> String.format( "-[%s]-", identifier );
            default -> identifier;
        };
    }


    @Override
    public String visitLocalRef( RexLocalRef localRef ) {
        String name = afterFields.get( localRef.getIndex() ).getName();
        if ( mapping.containsKey( name ) ) {
            return mapping.get( name );
        }
        return mappingLabel == null ? name : quoteIdentifier( name );
    }


    @Override
    public String visitCorrelVariable( RexCorrelVariable correlVariable ) {
        String name = afterFields.get( correlVariable.id.getId() ).getName();
        if ( mapping.containsKey( name ) ) {
            return mapping.get( name );
        }
        String identifier = mappingLabel == null ? name : quoteIdentifier( name );
        PolyType type = afterFields.get( correlVariable.id.getId() ).getType().getPolyType();
        return type == PolyType.NODE ? String.format( "(%s)", identifier ) : identifier;
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
        Function1<List<String>, String> getter = NeoUtil.getOpAsNeo( call.op.getOperatorName(), call.operands, call.type, this.beforeFields );
        assert getter != null : "Function is not supported by the Neo4j adapter.";
        String adjusted = getter.apply( IntStream.range( 0, ops.size() ).mapToObj( i -> call.operands.get( i ).getType().getPolyType() == PolyType.DECIMAL ? "toFloat(" + ops.get( i ) + ")" : ops.get( i ) ).toList() );
        if ( useBrackets ) {
            return "(" + adjusted + ")";
        }
        return " " + adjusted + " ";
    }


    @Override
    public String visitFieldAccess( RexFieldAccess fieldAccess ) {
        if ( mapping != null ) {
            return mapping.get( fieldAccess.getField().getName() );
        }
        return fieldAccess.getField().getName();
    }


    private String handleBinaries( RexCall call ) {
        List<String> nodes = call.operands.stream().map( o -> o.accept( this ) ).toList();

        return getFinalFunction( call, nodes );

    }


    private String handleExtractFromPath( RexCall call ) {
        assert call.operands.get( 1 ).isA( Kind.LITERAL );

        return quoteIdentifier( ((RexLiteral) call.operands.get( 1 )).value.asString().value );
    }


    private String handleSetProperty( RexCall call ) {
        String identifier = call.operands.get( 0 ).accept( this );
        String key = call.operands.get( 1 ).unwrapOrThrow( RexLiteral.class ).value.asString().value;
        String literal = NeoStatements.graphProperty_( call.operands.get( 2 ).unwrapOrThrow( RexLiteral.class ).value ).build();
        return String.format( "%s.%s = %s", identifier, quoteIdentifier( key ), literal );
    }


    private String handleSetLabels( RexCall call ) {
        String identifier = call.operands.get( 0 ).accept( this );
        List<PolyValue> rexLabels = ((RexLiteral) call.operands.get( 1 )).value.asList();
        List<String> labels = rexLabels.stream().map( l -> ":" + quoteIdentifier( l.asString().value ) ).toList();
        return identifier + String.join( "", labels );
    }

}
