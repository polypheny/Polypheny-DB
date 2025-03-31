/*
 * Copyright 2019-2020 The Polypheny Project
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

package org.polypheny.db.util.mapping;


import java.util.Iterator;
import org.polypheny.db.util.mapping.Mappings.TargetMapping;


/**
 * Simple implementation of {@link TargetMapping} where the number of sources and targets are specified as constructor parameters and you
 * just need to implement one method,
 */
public abstract class AbstractSourceMapping extends Mappings.AbstractMapping implements Mapping {

    private final int sourceCount;
    private final int targetCount;


    public AbstractSourceMapping( int sourceCount, int targetCount ) {
        this.sourceCount = sourceCount;
        this.targetCount = targetCount;
    }


    @Override
    public int getSourceCount() {
        return sourceCount;
    }


    @Override
    public int getTargetCount() {
        return targetCount;
    }


    @Override
    public Mapping inverse() {
        return Mappings.invert( this );
    }


    @Override
    public int size() {
        return targetCount;
    }


    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }


    @Override
    public MappingType getMappingType() {
        return MappingType.INVERSE_PARTIAL_FUNCTION;
    }


    @Override
    public Iterator<IntPair> iterator() {
        return new Iterator<IntPair>() {
            int source;
            int target = -1;


            {
                moveToNext();
            }


            private void moveToNext() {
                while ( ++target < targetCount ) {
                    source = getSourceOpt( target );
                    if ( source >= 0 ) {
                        break;
                    }
                }
            }


            @Override
            public boolean hasNext() {
                return target < targetCount;
            }


            @Override
            public IntPair next() {
                IntPair p = new IntPair( source, target );
                moveToNext();
                return p;
            }


            @Override
            public void remove() {
                throw new UnsupportedOperationException( "remove" );
            }
        };
    }


    @Override
    public abstract int getSourceOpt( int source );

}
