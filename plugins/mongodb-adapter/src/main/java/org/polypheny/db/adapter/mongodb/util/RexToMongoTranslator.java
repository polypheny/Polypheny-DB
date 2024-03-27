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

package org.polypheny.db.adapter.mongodb.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.adapter.mongodb.MongoAlg.Implementor;
import org.polypheny.db.adapter.mongodb.bson.BsonDynamic;
import org.polypheny.db.adapter.mongodb.bson.BsonKeyValue;
import org.polypheny.db.adapter.mongodb.rules.MongoRules;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.operators.OperatorName;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.languages.OperatorRegistry;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.nodes.Operator;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexFieldAccess;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNameRef;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.rex.RexVisitorImpl;
import org.polypheny.db.type.PolyType;
import org.polypheny.db.util.Util;

/**
 * Translator from {@link RexNode} to strings in MongoDB's expression language.
 */
public class RexToMongoTranslator extends RexVisitorImpl<String> {

    private final List<String> inFields;

    static final Map<Operator, String> MONGO_OPERATORS = new HashMap<>();


    static {
        // Arithmetic
        MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.DIVIDE ), "$divide" );
        MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.MULTIPLY ), "$multiply" );
        MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.MOD ), "$mod" );
        MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.PLUS ), "$add" );
        MONGO_OPERATORS.put( OperatorRegistry.get( QueryLanguage.from( "mql" ), OperatorName.PLUS ), "$add" );
        MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.MINUS ), "$subtract" );
        // Boolean
        MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.AND ), "$and" );
        MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.OR ), "$or" );
        MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.NOT ), "$not" );
        // Comparison
        MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.EQUALS ), "$eq" );
        MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.NOT_EQUALS ), "$ne" );
        MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.GREATER_THAN ), "$gt" );
        MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.GREATER_THAN_OR_EQUAL ), "$gte" );
        MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.LESS_THAN ), "$lt" );
        MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.LESS_THAN_OR_EQUAL ), "$lte" );

        MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.FLOOR ), "$floor" );
        MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.CEIL ), "$ceil" );
        MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.EXP ), "$exp" );
        MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.LN ), "$ln" );
        MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.LOG10 ), "$log10" );
        MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.ABS ), "$abs" );
        MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.CHAR_LENGTH ), "$strLenCP" );
        MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.SUBSTRING ), "$substrCP" );
        MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.ROUND ), "$round" );
        MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.ACOS ), "$acos" );
        MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.TAN ), "$tan" );
        MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.COS ), "$cos" );
        MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.ASIN ), "$asin" );
        MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.SIN ), "$sin" );
        MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.ATAN ), "$atan" );
        MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.ATAN2 ), "$atan2" );

        MONGO_OPERATORS.put( OperatorRegistry.get( OperatorName.POWER ), "$pow" );
    }


    private final Implementor implementor;
    private final DataModel model;


    public RexToMongoTranslator( List<String> inFields, Implementor implementor, DataModel model ) {
        super( true );
        this.implementor = implementor;
        this.inFields = inFields;
        this.model = model;
    }


    @Override
    public String visitLiteral( RexLiteral literal ) {
        if ( literal.getValue() == null ) {
            return "null";
        }
        return "{$literal: " + (literal.value.isString() ? literal.value.asString().toQuotedJson() : literal.value.toJson()) + "}";
    }


    @Override
    public String visitDynamicParam( RexDynamicParam dynamicParam ) {
        return new BsonDynamic( dynamicParam ).toJson();
    }


    @Override
    public String visitIndexRef( RexIndexRef inputRef ) {
        if ( model == DataModel.DOCUMENT ) {
            return "1";
        }
        implementor.physicalMapper.add( inFields.get( inputRef.getIndex() ) );
        return MongoRules.maybeQuote( "$" + inFields.get( inputRef.getIndex() ) );
    }


    @Override
    public String visitNameRef( RexNameRef nameRef ) {
        return MongoRules.maybeQuote( "$" + nameRef.name );
    }


    @Override
    public String visitCall( RexCall call ) {
        String name = MongoRules.isItem( call );
        if ( name != null ) {
            return "'$" + name + "'";
        }
        final List<String> strings = translateList( call.operands );
        if ( call.getKind() == Kind.CAST ) {
            return strings.get( 0 );
        }
        String stdOperator = MONGO_OPERATORS.get( call.getOperator() );
        if ( stdOperator != null ) {
            if ( call.getOperator().equals( OperatorRegistry.get( OperatorName.SUBSTRING ) ) ) {
                String first = strings.get( 1 );
                first = "{\"$subtract\":[" + first + ", 1]}";
                strings.remove( 1 );
                strings.add( first );
                if ( call.getOperands().size() == 2 ) {
                    strings.add( " { \"$strLenCP\":" + strings.get( 0 ) + "}" );
                }
            }
            return "{" + stdOperator + ": [" + Util.commaList( strings ) + "]}";
        }
        if ( call.getOperator().getOperatorName() == OperatorName.ITEM ) {
            final RexNode op1 = call.operands.get( 1 );
            // normal
            if ( op1 instanceof RexLiteral && op1.getType().getPolyType() == PolyType.INTEGER ) {
                return "{$arrayElemAt:[" + strings.get( 0 ) + "," + (((RexLiteral) op1).value.asNumber().intValue() - 1) + "]}";
            }
            // prepared
            if ( op1 instanceof RexDynamicParam ) {
                return "{$arrayElemAt:[" + strings.get( 0 ) + ", {$subtract:[" + new BsonDynamic( (RexDynamicParam) op1 ).toJson() + ", 1]}]}";
            }
        }
        if ( call.getOperator().equals( OperatorRegistry.get( OperatorName.CASE ) ) ) {
            StringBuilder sb = new StringBuilder();
            StringBuilder finish = new StringBuilder();
            // case(a, b, c)  -> $cond:[a, b, c]
            // case(a, b, c, d) -> $cond:[a, b, $cond:[c, d, null]]
            // case(a, b, c, d, e) -> $cond:[a, b, $cond:[c, d, e]]
            for (
                    int i = 0; i < strings.size(); i += 2 ) {
                sb.append( "{$cond:[" );
                finish.append( "]}" );

                sb.append( strings.get( i ) );
                sb.append( ',' );
                sb.append( strings.get( i + 1 ) );
                sb.append( ',' );
                if ( i == strings.size() - 3 ) {
                    sb.append( strings.get( i + 2 ) );
                    break;
                }
                if ( i == strings.size() - 2 ) {
                    sb.append( "null" );
                    break;
                }
            }
            sb.append( finish );
            return sb.toString();
        }
        if ( call.op.equals( OperatorRegistry.get( OperatorName.UNARY_MINUS ) ) ) {
            if ( strings.size() == 1 ) {
                return "{\"$multiply\":[" + strings.get( 0 ) + ",-1]}";
            }
        }

        String special = handleSpecialCases( call );
        if ( special != null ) {
            return special;
        }

        if ( call.op.getOperatorName() == OperatorName.SIGN ) {
            // x < 0, -1
            // x == 0, 0
            // x > 0, 1
            StringBuilder sb = new StringBuilder();
            String oper = call.operands.get( 0 ).accept( this );
            sb.append( """
                    {"$switch":
                                {
                                  "branches": [
                                    {
                                      "case": { "$lt" : [\s""" );
            sb.append( oper );
            sb.append( """
                    , 0 ] },
                                      "then": -1.0                },
                                    {
                                      "case": { "$gt" : [\s""" );
            sb.append( oper );
            sb.append( """
                    , 0 ] },
                                      "then": 1.0                },
                                  ],
                                  "default": 0.0            }}""" );

            return sb.toString();
        }

        if ( call.op.equals( OperatorRegistry.get( OperatorName.IS_NOT_NULL ) ) ) {
            return call.operands.get( 0 ).accept( this );

        }

        throw new IllegalArgumentException( "Translation of " + call + " is not supported by MongoProject" );

    }


    public String handleSpecialCases( RexCall call ) {
        if ( call.getType().getPolyType() == PolyType.ARRAY ) {
            BsonArray array = new BsonArray();
            array.addAll( translateList( call.operands ).stream().map( BsonString::new ).toList() );
            return array.toString();
        } else if ( call.isA( Kind.MQL_ITEM ) ) {
            RexNode leftPre = call.operands.get( 0 );
            String left = leftPre.accept( this );

            String right = call.operands.get( 1 ).accept( this );

            return "{\"$arrayElemAt\":[" + left + "," + right + "]}";
        } else if ( call.isA( Kind.MQL_SLICE ) ) {
            String left = call.operands.get( 0 ).accept( this );
            String skip = call.operands.get( 1 ).accept( this );
            String return_ = call.operands.get( 2 ).accept( this );

            return "{\"$slice\":[ " + left + "," + skip + "," + return_ + "]}";
        } else if ( call.isA( Kind.MQL_EXCLUDE ) ) {
            String parent = implementor
                    .getTupleType()
                    .getFieldNames()
                    .get( ((RexIndexRef) call.operands.get( 0 )).getIndex() );

            if ( !(call.operands.get( 1 ) instanceof RexCall excludes) || call.operands.size() != 2 ) {
                return null;
            }
            List<String> fields = new ArrayList<>();
            for ( RexNode operand : excludes.operands ) {
                if ( !(operand instanceof RexCall) ) {
                    return null;
                }
                fields.add( "\"" + parent + "." + ((RexCall) operand)
                        .operands
                        .stream()
                        .map( op -> ((RexLiteral) op).value.asString().value )
                        .collect( Collectors.joining( "." ) ) + "\": 0" );
            }

            return String.join( ",", fields );
        } else if ( call.isA( Kind.UNWIND ) ) {
            return call.operands.get( 0 ).accept( this );
        } else if ( call.isA( Kind.MQL_ADD_FIELDS ) ) {
            return handleAddFieldsFunctions( call );
        } else if ( call.op.getOperatorName() == OperatorName.MQL_REPLACE_ROOT ) {
            return handleWithFunctions( call );
        }
        return null;
    }


    @NotNull
    private String handleAddFieldsFunctions( RexCall call ) {
        String names = call.operands.get( 1 ).accept( this );
        String value = call.operands.get( 2 ).accept( this );
        if ( call.operands.get( 1 ).isA( Kind.DYNAMIC_PARAM ) ) {
            BsonKeyValue kv = new BsonKeyValue( BsonDynamic.changeType( BsonDynamic.addFunction( BsonDocument.parse( names ), "joinPoint" ), PolyType.VARCHAR ), BsonDocument.parse( value ) );
            return "{\"$addFields\": {" + MongoRules.maybeQuote( kv.placeholderKey ) + ":" + kv.toJson() + "}}";
        }
        return "{\"$addFields\": {" + MongoRules.maybeQuote( names ) + ":" + value + "}}";
    }


    @NotNull
    private String handleWithFunctions( RexCall call ) {
        String names = call.operands.get( 0 ).accept( this );
        if ( call.operands.get( 0 ).isA( Kind.DYNAMIC_PARAM ) ) {
            BsonDocument key = BsonDynamic.changeType( BsonDynamic.addFunction( BsonDocument.parse( names ), "joinPoint" ), PolyType.VARCHAR );
            return "{\"$replaceWith\": " + key.toJson() + "}";
        }
        return "{\"$replaceWith\": " + names + "}";
    }


    public List<String> translateList( List<RexNode> list ) {
        final List<String> strings = new ArrayList<>();
        for ( RexNode node : list ) {
            strings.add( node.accept( this ) );
        }
        return strings;
    }


    @Override
    public String visitFieldAccess( RexFieldAccess fieldAccess ) {
        return MongoRules.maybeQuote( "$" + fieldAccess.getField().getName() );
    }

}
