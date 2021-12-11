/*
 * Copyright 2019-2021 The Polypheny Project
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


/**
 * Interpreter node that implements a {@link Join}.
 */
public class JoinNode implements Node {

    private final Source leftSource;
    private final Source rightSource;
    private final Sink sink;
    private final Join alg;
    private final Scalar condition;
    private final Context context;


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
        List<Row> rightList = null;
        final int leftCount = alg.getLeft().getRowType().getFieldCount();
        final int rightCount = alg.getRight().getRowType().getFieldCount();
        context.values = new Object[alg.getRowType().getFieldCount()];
        Row left;
        Row right;
        while ( (left = leftSource.receive()) != null ) {
            System.arraycopy( left.getValues(), 0, context.values, 0, leftCount );
            if ( rightList == null ) {
                rightList = new ArrayList<>();
                while ( (right = rightSource.receive()) != null ) {
                    rightList.add( right );
                }
            }
            for ( Row right2 : rightList ) {
                System.arraycopy( right2.getValues(), 0, context.values, leftCount, rightCount );
                final Boolean execute = (Boolean) condition.execute( context );
                if ( execute != null && execute ) {
                    sink.send( Row.asCopy( context.values ) );
                }
            }
        }
    }

}

