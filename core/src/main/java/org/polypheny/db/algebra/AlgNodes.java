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

package org.polypheny.db.algebra;


import com.google.common.collect.Ordering;
import java.util.Comparator;
import org.polypheny.db.runtime.Utilities;


/**
 * Utilities concerning relational expressions.
 */
public class AlgNodes {

    /**
     * Comparator that provides an arbitrary but stable ordering to {@link AlgNode}s.
     */
    public static final Comparator<AlgNode> COMPARATOR = new AlgNodeComparator();

    /**
     * Ordering for {@link AlgNode}s.
     */
    public static final Ordering<AlgNode> ORDERING = Ordering.from( COMPARATOR );


    private AlgNodes() {
    }


    /**
     * Compares arrays of {@link AlgNode}.
     */
    public static int compareAlgs( AlgNode[] algs0, AlgNode[] algs1 ) {
        int c = Utilities.compare( algs0.length, algs1.length );
        if ( c != 0 ) {
            return c;
        }
        for ( int i = 0; i < algs0.length; i++ ) {
            c = COMPARATOR.compare( algs0[i], algs1[i] );
            if ( c != 0 ) {
                return c;
            }
        }
        return 0;
    }


    /**
     * Arbitrary stable comparator for {@link AlgNode}s.
     */
    private static class AlgNodeComparator implements Comparator<AlgNode> {

        @Override
        public int compare( AlgNode o1, AlgNode o2 ) {
            // Compare on field count first. It is more stable than id (when rules are added to the set of active rules).
            final int c = Utilities.compare( o1.getTupleType().getFieldCount(), o2.getTupleType().getFieldCount() );
            if ( c != 0 ) {
                return -c;
            }
            return Utilities.compare( o1.getId(), o2.getId() );
        }

    }

}

