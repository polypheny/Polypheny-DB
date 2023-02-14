/*
 * Copyright 2019-2023 The Polypheny Project
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

package org.polypheny.db.adaptimizer.polyfierconnect.pseudo.struct;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.polypheny.db.util.Pair;

import java.io.Serializable;
import java.util.Random;
import java.util.Stack;

@Getter(AccessLevel.PRIVATE)
@Setter(AccessLevel.PRIVATE)
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class RandomNode implements Serializable {
    private RandomNode source;

    private long id;
    private transient Random random;
    private Stack<Pair<String, Long>> seeds;

    public static RandomNode root( long seed ) {
        RandomNode randomNode = new RandomNode();
        randomNode.source = null;
        randomNode.random = new Random( seed );
        randomNode.seeds = new Stack<>();
        randomNode.seeds.push( Pair.of( "root", seed ) );
        return randomNode;
    }

    public long get() {
        return getSeeds().peek().right;
    }

    public RandomNode branch( String origin ) {
        RandomNode randomNode = new RandomNode();
        randomNode.source = this;
        randomNode.random = new Random( get() + 1 );
        randomNode.seeds = new Stack<>();
        randomNode.seeds.push( Pair.of( origin, get() + 1 ) );
        return randomNode;
    }

    public Random getRng() {
        return new Random( get() );
    }

}
