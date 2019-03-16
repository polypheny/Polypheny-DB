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

package ch.unibas.dmi.dbis.polyphenydb.util.mapping;


import ch.unibas.dmi.dbis.polyphenydb.util.mapping.Mappings.TargetMapping;
import java.util.Iterator;


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


    public Mapping inverse() {
        return Mappings.invert( this );
    }


    public int size() {
        return targetCount;
    }


    public void clear() {
        throw new UnsupportedOperationException();
    }


    public MappingType getMappingType() {
        return MappingType.INVERSE_PARTIAL_FUNCTION;
    }


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


            public boolean hasNext() {
                return target < targetCount;
            }


            public IntPair next() {
                IntPair p = new IntPair( source, target );
                moveToNext();
                return p;
            }


            public void remove() {
                throw new UnsupportedOperationException( "remove" );
            }
        };
    }


    public abstract int getSourceOpt( int source );
}

