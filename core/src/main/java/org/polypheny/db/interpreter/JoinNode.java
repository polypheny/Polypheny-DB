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

package org.polypheny.db.interpreter;


import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.polypheny.db.algebra.core.Join;
import org.polypheny.db.type.entity.PolyValue;


/**
 * Interpreter node that implements a {@link Join}.
 */
public class JoinNode implements Node {

    private final Source<PolyValue> leftSource;
    private final Source<PolyValue> rightSource;
    private final Sink sink;
    private final Join alg;
    private final Scalar condition;
    private final Context<PolyValue> context;


    public JoinNode( Compiler compiler, Join alg ) {
        this.leftSource = compiler.source( alg, 0 );
        this.rightSource = compiler.source( alg, 1 );
        this.sink = compiler.sink( alg );
        this.condition = compiler.compile( ImmutableList.of( alg.getCondition() ), compiler.combinedRowType( alg.getInputs() ) );
        this.alg = alg;
        this.context = compiler.createContext();

    }


    @Override
    public void run() throws InterruptedException {
        List<Row<PolyValue>> rightList = null;
        final int leftCount = alg.getLeft().getTupleType().getFieldCount();
        final int rightCount = alg.getRight().getTupleType().getFieldCount();
        context.values = new PolyValue[alg.getTupleType().getFieldCount()];
        Row<PolyValue> left;
        Row<PolyValue> right;
        while ( (left = leftSource.receive()) != null ) {
            System.arraycopy( left.getValues(), 0, context.values, 0, leftCount );
            if ( rightList == null ) {
                rightList = new ArrayList<>();
                while ( (right = rightSource.receive()) != null ) {
                    rightList.add( right );
                }
            }
            for ( Row<PolyValue> right2 : rightList ) {
                System.arraycopy( right2.getValues(), 0, context.values, leftCount, rightCount );
                final Boolean execute = condition.execute( context ).asBoolean().value;
                if ( execute != null && execute ) {
                    sink.send( Row.asCopy( context.values ) );
                }
            }
        }
    }

}

