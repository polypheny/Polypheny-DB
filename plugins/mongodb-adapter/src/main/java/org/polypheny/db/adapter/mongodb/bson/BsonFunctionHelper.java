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

package org.polypheny.db.adapter.mongodb.bson;

import com.google.common.collect.ImmutableList;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.polypheny.db.adapter.mongodb.MongoAlg.Implementor;
import org.polypheny.db.algebra.constant.Kind;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.rex.RexCall;
import org.polypheny.db.rex.RexDynamicParam;
import org.polypheny.db.rex.RexIndexRef;
import org.polypheny.db.rex.RexLiteral;
import org.polypheny.db.rex.RexNode;
import org.polypheny.db.util.BsonUtil;

public class BsonFunctionHelper extends BsonDocument {

    static String decimalCast = "if( arr1.length > 1) {\n"
            + "                if ( arr1[0].toString().includes('NumberDecimal')) {\n"
            + "                    arr1 = arr1.map( a => parseFloat(a.toString().replace('NumberDecimal(\\\"','').replace('\\\")', '')))\n"
            + "                }\n"
            + "            }\n"
            + "            if( arr1.length > 1) {\n"
            + "                if ( arr2[0].toString().includes('NumberDecimal')) {\n"
            + "                    arr2 = arr2.map( a => parseFloat(a.toString().replace('NumberDecimal(\\\"','').replace('\\\")', '')))\n"
            + "                }\n"
            + "            }";

    static String l2Function = "function(arr1, arr2){"
            + decimalCast
            + "        var result = 0;\n"
            + "        for ( var i = 0; i < arr1.length; i++){\n"
            + "            result += Math.pow(arr1[i] - arr2[i], 2);\n"
            + "        }\n"
            + "        return result;}";
    static String l2squaredFunction = "function(arr1, arr2){"
            + decimalCast
            + "        var result = 0;\n"
            + "        for ( var i = 0; i < arr1.length; i++){\n"
            + "            result += Math.pow(arr1[i] - arr2[i], 2);\n"
            + "        }\n"
            + "        return Math.sqrt(result);}";
    static String l1Function = "function(arr1, arr2){"
            + decimalCast
            + "        var result = 0;\n"
            + "        for ( var i = 0; i < arr1.length; i++){\n"
            + "            result += Math.abs(arr1[i] - arr2[i]);\n"
            + "        }\n"
            + "        return result;}";
    static String chiFunction = "function(arr1, arr2){"
            + decimalCast
            + "        var result = 0;\n"
            + "        for ( var i = 0; i < arr1.length; i++){\n"
            + "            result += Math.pow(arr1[i] - arr2[i], 2)/(arr1[i] + arr2[i]);\n"
            + "        }\n"
            + "        return result;}";


    public static BsonDocument getFunction( RexCall call, AlgDataType rowType, Implementor implementor ) {
        String function;
        if ( call.operands.size() == 3 && call.operands.get( 2 ) instanceof RexLiteral ) {
            Object funcName = ((RexLiteral) call.operands.get( 2 )).getValue();
            function = getUsedFunction( funcName );

            return new BsonDocument().append(
                    "$function",
                    new BsonDocument()
                            .append( "body", new BsonString( function ) )
                            .append( "args", getArgsArray( call.operands, rowType, implementor ) )
                            .append( "lang", new BsonString( "js" ) ) );

        }
        if ( call.operands.size() == 3 ) {
            // prepared
            return new BsonDocument().append( "$function", new BsonDocument()
                    .append( "body", getDynamicFunction( call.operands.get( 2 ) ) )
                    .append( "args", getArgsArray( call.operands, rowType, implementor ) )
                    .append( "lang", new BsonString( "js" ) ) );
        }
        throw new IllegalArgumentException( "Unsupported function for MongoDB" );
    }


    private static BsonValue getDynamicFunction( RexNode rexNode ) {
        if ( rexNode.isA( Kind.DYNAMIC_PARAM ) ) {
            return new BsonDynamic( (RexDynamicParam) rexNode ).setIsFunc( true );
        } else if ( rexNode.isA( Kind.CAST ) ) {
            RexCall call = (RexCall) rexNode;
            return getDynamicFunction( call.operands.get( 0 ) );
        }
        throw new IllegalArgumentException( "Unsupported dynamic parameter for MongoDB" );
    }


    public static String getUsedFunction( Object funcName ) {
        String function;
        if ( funcName.equals( "L2SQUARED" ) ) {
            function = l2Function;
        } else if ( funcName.equals( "L2" ) ) {
            function = l2squaredFunction;
        } else if ( funcName.equals( "L1" ) ) {
            function = l1Function;
        } else if ( funcName.equals( "CHISQUARED" ) ) {
            function = chiFunction;
        } else {
            throw new IllegalArgumentException( "Unsupported function for MongoDB" );
        }
        return function;
    }


    private static BsonArray getArgsArray( ImmutableList<RexNode> operands, AlgDataType rowType, Implementor implementor ) {
        BsonArray array = new BsonArray();
        if ( operands.size() == 3 ) {
            array.add( getVal( operands.get( 0 ), rowType, implementor ) );
            array.add( getVal( operands.get( 1 ), rowType, implementor ) );
            return array;
        }
        throw new IllegalArgumentException( "This function is not supported yet" );
    }


    private static BsonValue getVal( RexNode rexNode, AlgDataType rowType, Implementor implementor ) {
        if ( rexNode.isA( Kind.INPUT_REF ) ) {
            RexIndexRef rex = (RexIndexRef) rexNode;
            return new BsonString( "$" + rowType.getFields().get( rex.getIndex() ).getPhysicalName() );
        } else if ( rexNode.isA( Kind.ARRAY_VALUE_CONSTRUCTOR ) ) {
            RexCall rex = (RexCall) rexNode;
            return BsonUtil.getBsonArray( rex, implementor.getBucket() );
        } else if ( rexNode.isA( Kind.DYNAMIC_PARAM ) ) {
            RexDynamicParam rex = (RexDynamicParam) rexNode;
            return new BsonDynamic( rex );
        }
        throw new IllegalArgumentException( "This function argument is not supported yet" );
    }

}
