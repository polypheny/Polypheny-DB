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

package org.polypheny.db.cypher.expression;

import com.google.common.collect.Maps;
import com.google.common.collect.Streams;
import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.Getter;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.catalog.exceptions.GenericRuntimeException;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.RexType;
import org.polypheny.db.cypher.parser.StringPos;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeUtil;
import org.polypheny.db.type.entity.PolyBoolean;
import org.polypheny.db.type.entity.PolyList;
import org.polypheny.db.type.entity.PolyString;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.graph.PolyDictionary;
import org.polypheny.db.type.entity.numerical.PolyDouble;
import org.polypheny.db.type.entity.numerical.PolyInteger;
import org.polypheny.db.util.Pair;

@Getter
public class CypherLiteral extends CypherExpression {

    private final Literal literalType;
    private Map<String, CypherExpression> mapValue;
    private Object value;
    private List<CypherExpression> listValue;


    public CypherLiteral( ParserPos pos, Literal literalType ) {
        super( pos );
        this.literalType = literalType;
        this.value = null;
    }


    public CypherLiteral( ParserPos pos, Literal literalType, List<CypherExpression> list ) {
        super( pos );
        this.literalType = literalType;
        assert literalType == Literal.LIST;
        this.listValue = list;
    }


    public CypherLiteral( ParserPos pos, Literal literalType, String string ) {
        super( pos );
        this.literalType = literalType;
        assert literalType == Literal.STRING;
        this.value = string;
    }


    public CypherLiteral( ParserPos pos, Literal literalType, List<StringPos> keys, List<CypherExpression> values ) {
        super( pos );
        this.literalType = literalType;
        assert keys.size() == values.size();
        //noinspection UnstableApiUsage
        this.mapValue = Streams.zip( keys.stream(), values.stream(), Maps::immutableEntry ).collect( Collectors.toMap( k -> k.getKey().getImage(), Entry::getValue ) );
    }


    public CypherLiteral( ParserPos pos, Literal literalType, String image, boolean negated ) {
        super( pos );
        this.literalType = literalType;
        if ( literalType == Literal.DECIMAL ) {
            this.value = Integer.parseInt( image ) * (negated ? -1 : 1);
        } else if ( (literalType == Literal.DOUBLE) ) {
            this.value = Double.parseDouble( image ) * (negated ? -1 : 1);
        } else {
            throw new GenericRuntimeException( "Could not use provided format to creat cypher literal." );
        }
    }


    public enum Literal {
        TRUE, FALSE, NULL, LIST, MAP, STRING, DOUBLE, DECIMAL, HEX, OCTAL, STAR
    }


    @Override
    public PolyValue getComparable() {

        return switch ( literalType ) {
            case TRUE -> PolyBoolean.of( true );
            case FALSE -> PolyBoolean.of( false );
            case NULL -> null;
            case LIST -> {
                List<PolyValue> list = listValue.stream().map( CypherExpression::getComparable ).toList();
                yield new PolyList<>( list );
            }
            case MAP -> {
                Map<PolyString, PolyValue> map = mapValue.entrySet().stream().collect( Collectors.toMap( e -> PolyString.of( e.getKey() ), e -> e.getValue().getComparable() ) );
                yield new PolyDictionary( map );
            }
            case STRING, HEX, OCTAL -> PolyString.of( (String) value );
            case DOUBLE -> PolyDouble.of( (Double) value );
            case DECIMAL -> PolyInteger.of( (Integer) value );
            case STAR -> throw new UnsupportedOperationException();
        };
    }


    @Override
    public Pair<PolyString, RexNode> getRex( CypherContext context, RexType type ) {
        RexNode node;
        switch ( literalType ) {
            case TRUE:
            case FALSE:
                node = context.rexBuilder.makeLiteral( (Boolean) value );
                break;
            case NULL:
                node = context.rexBuilder.makeLiteral( null, context.typeFactory.createPolyType( PolyType.VARCHAR, 255 ), false );
                break;
            case LIST:
                List<RexNode> list = listValue.stream().map( e -> e.getRex( context, type ).right ).toList();
                AlgDataType dataType = context.typeFactory.createPolyType( PolyType.ANY );

                if ( !list.isEmpty() && list.stream().allMatch( e -> PolyTypeUtil.equalSansNullability( context.typeFactory, e.getType(), list.get( 0 ).getType() ) ) ) {
                    dataType = list.get( 0 ).getType();
                }
                dataType = context.typeFactory.createArrayType( dataType, -1 );
                node = context.rexBuilder.makeLiteral( PolyList.copyOf( list.stream().map( e -> ((RexLiteral) e).value ).toList() ), dataType, false );
                break;
            case MAP:
            case STAR:
            case OCTAL:
            case HEX:
                throw new UnsupportedOperationException();
            case STRING:
                node = context.rexBuilder.makeLiteral( (String) value );
                break;
            case DOUBLE:
                node = context.rexBuilder.makeApproxLiteral( BigDecimal.valueOf( (Double) value ) );
                break;
            case DECIMAL:
                node = context.rexBuilder.makeExactLiteral( BigDecimal.valueOf( (Integer) value ) );
                break;
            default:
                throw new IllegalStateException( "Unexpected value: " + literalType );
        }
        return Pair.of( null, node );
    }


    @Override
    public ExpressionType getType() {
        return ExpressionType.LITERAL;
    }

}
