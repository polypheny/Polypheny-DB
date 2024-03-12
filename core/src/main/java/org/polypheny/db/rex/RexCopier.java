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

package org.polypheny.db.rex;


import org.polypheny.db.algebra.type.AlgDataType;


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


    private AlgDataType copy( AlgDataType type ) {
        return builder.getTypeFactory().copyType( type );
    }


    @Override
    public RexNode visitOver( RexOver over ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public RexWindow visitWindow( RexWindow window ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public RexNode visitCall( final RexCall call ) {
        final boolean[] update = null;
        return builder.makeCall( copy( call.getType() ), call.getOperator(), visitList( call.getOperands(), update ) );
    }


    @Override
    public RexNode visitCorrelVariable( RexCorrelVariable variable ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public RexNode visitFieldAccess( RexFieldAccess fieldAccess ) {
        return builder.makeFieldAccess( fieldAccess.getReferenceExpr().accept( this ), fieldAccess.getField().getIndex() );
    }


    @Override
    public RexNode visitIndexRef( RexIndexRef inputRef ) {
        return builder.makeInputRef( copy( inputRef.getType() ), inputRef.getIndex() );
    }


    @Override
    public RexNode visitLocalRef( RexLocalRef localRef ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public RexNode visitLiteral( RexLiteral literal ) {
        // Get the value as is
        return new RexLiteral( RexLiteral.value( literal ), copy( literal.getType() ), literal.getPolyType() );
    }


    @Override
    public RexNode visitDynamicParam( RexDynamicParam dynamicParam ) {
        throw new UnsupportedOperationException();
    }


    @Override
    public RexNode visitRangeRef( RexRangeRef rangeRef ) {
        throw new UnsupportedOperationException();
    }

}

