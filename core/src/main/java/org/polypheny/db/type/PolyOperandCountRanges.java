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

package org.polypheny.db.type;


import com.google.common.base.Preconditions;
import java.io.Serializable;


/**
 * Helpers for {@link OperandCountRange}.
 */
public abstract class PolyOperandCountRanges {

    public static OperandCountRange of( int length ) {
        return new RangeImpl( length, length );
    }


    public static OperandCountRange between( int min, int max ) {
        return new RangeImpl( min, max );
    }


    public static OperandCountRange from( int min ) {
        return new RangeImpl( min, -1 );
    }


    public static OperandCountRange any() {
        return new RangeImpl( 0, -1 );
    }


    /**
     * Implementation of {@link OperandCountRange}.
     */
    private static class RangeImpl implements OperandCountRange, Serializable {

        private final int min;
        private final int max;


        RangeImpl( int min, int max ) {
            this.min = min;
            this.max = max;
            Preconditions.checkArgument( min <= max || max == -1 );
            Preconditions.checkArgument( min >= 0 );
        }


        @Override
        public boolean isValidCount( int count ) {
            return count >= min && (max == -1 || count <= max);
        }


        @Override
        public int getMin() {
            return min;
        }


        @Override
        public int getMax() {
            return max;
        }

    }

}

