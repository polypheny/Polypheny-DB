/*
 * This file is based on code taken from the Apache Calcite project, which was released under the Apache License.
 * The changes are released under the MIT license.
 *
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
 *
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package ch.unibas.dmi.dbis.polyphenydb.rex;


import ch.unibas.dmi.dbis.polyphenydb.rel.type.RelDataType;


/**
 * Shuttle which creates a deep copy of a Rex expression.
 *
 * This is useful when copying objects from one type factory or builder to another.
 *
 * Due to the laziness of the author, not all Rex types are supported at present.
 *
 * @see RexBuilder#copy(RexNode)
 */
class RexCopier extends RexShuttle {

    private final RexBuilder builder;


    /**
     * Creates a RexCopier.
     *
     * @param builder Builder
     */
    RexCopier( RexBuilder builder ) {
        this.builder = builder;
    }


    private RelDataType copy( RelDataType type ) {
        return builder.getTypeFactory().copyType( type );
    }


    public RexNode visitOver( RexOver over ) {
        throw new UnsupportedOperationException();
    }


    public RexWindow visitWindow( RexWindow window ) {
        throw new UnsupportedOperationException();
    }


    public RexNode visitCall( final RexCall call ) {
        final boolean[] update = null;
        return builder.makeCall( copy( call.getType() ), call.getOperator(), visitList( call.getOperands(), update ) );
    }


    public RexNode visitCorrelVariable( RexCorrelVariable variable ) {
        throw new UnsupportedOperationException();
    }


    public RexNode visitFieldAccess( RexFieldAccess fieldAccess ) {
        return builder.makeFieldAccess( fieldAccess.getReferenceExpr().accept( this ), fieldAccess.getField().getIndex() );
    }


    public RexNode visitInputRef( RexInputRef inputRef ) {
        return builder.makeInputRef( copy( inputRef.getType() ), inputRef.getIndex() );
    }


    public RexNode visitLocalRef( RexLocalRef localRef ) {
        throw new UnsupportedOperationException();
    }


    public RexNode visitLiteral( RexLiteral literal ) {
        // Get the value as is
        return new RexLiteral( RexLiteral.value( literal ), copy( literal.getType() ), literal.getTypeName() );
    }


    public RexNode visitDynamicParam( RexDynamicParam dynamicParam ) {
        throw new UnsupportedOperationException();
    }


    public RexNode visitRangeRef( RexRangeRef rangeRef ) {
        throw new UnsupportedOperationException();
    }
}

