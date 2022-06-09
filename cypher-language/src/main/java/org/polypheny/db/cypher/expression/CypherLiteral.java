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
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.CypherContext;
import org.polypheny.db.cypher.cypher2alg.CypherToAlgConverter.RexType;
import org.polypheny.db.cypher.parser.StringPos;
import org.polypheny.db.languages.ParserPos;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.runtime.PolyCollections.PolyDictionary;
import org.polypheny.db.runtime.PolyCollections.PolyList;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.type.PolyTypeUtil;
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
            throw new RuntimeException( "Could not use provided format to creat cypher literal." );
        }
    }


    public enum Literal {
        TRUE, FALSE, NULL, LIST, MAP, STRING, DOUBLE, DECIMAL, HEX, OCTAL, STAR
    }


    @Override
    public Comparable<?> getComparable() {

        switch ( literalType ) {
            case TRUE:
                return true;
            case FALSE:
                return false;
            case NULL:
                return null;
            case LIST:
                List<Comparable<?>> list = listValue.stream().map( CypherExpression::getComparable ).collect( Collectors.toList() );
                return new PolyList<>( list );
            case MAP:
                Map<String, Comparable<?>> map = mapValue.entrySet().stream().collect( Collectors.toMap( Entry::getKey, e -> e.getValue().getComparable() ) );
                return new PolyDictionary( map );
            case STRING:
                return (String) value;
            case DOUBLE:
                return (Double) value;
            case DECIMAL:
                return (Integer) value;
            case HEX:
                return (String) value;
            case OCTAL:
                return (String) value;
            case STAR:
                throw new UnsupportedOperationException();
        }
        throw new UnsupportedOperationException();
    }


    @Override
    public Pair<String, RexNode> getRex( CypherContext context, RexType type ) {
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
                List<RexNode> list = listValue.stream().map( e -> e.getRex( context, type ).right ).collect( Collectors.toList() );
                AlgDataType dataType = context.typeFactory.createPolyType( PolyType.ANY );

                if ( !list.isEmpty() && list.stream().allMatch( e -> PolyTypeUtil.equalSansNullability( context.typeFactory, e.getType(), list.get( 0 ).getType() ) ) ) {
                    dataType = list.get( 0 ).getType();
                }
                dataType = context.typeFactory.createArrayType( dataType, -1 );
                node = context.rexBuilder.makeLiteral( list, dataType, false );
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
