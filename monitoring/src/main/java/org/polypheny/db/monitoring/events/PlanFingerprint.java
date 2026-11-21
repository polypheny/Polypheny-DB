/*
 * Copyright 2019-2026 The Polypheny Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.monitoring.events;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.polypheny.db.algebra.AlgNode;

/**
 * Identifies the ordered operator shape of a routed plan for monitoring.
 * Plans with the same operators and input structure share a fingerprint regardless of their arguments.
 */
public final class PlanFingerprint {

    private PlanFingerprint() {
    }


    /** Returns the fingerprint, or {@code null} if the input graph has a cycle or missing nodes. */
    @Nullable
    public static String of( AlgNode plan ) {
        if ( plan == null ) {
            return null;
        }

        // A null value marks a node on the current path. Non-null values can be reused for shared inputs.
        Map<AlgNode, HashCode> fingerprints = new IdentityHashMap<>();
        Deque<Frame> pending = new ArrayDeque<>();
        fingerprints.put( plan, null );
        pending.push( new Frame( plan ) );
        while ( !pending.isEmpty() ) {
            Frame frame = pending.peek();
            if ( frame.inputs == null ) {
                return null;
            }
            if ( !frame.remaining.hasNext() ) {
                fingerprints.put( frame.node, fingerprint( frame, fingerprints ) );
                pending.pop();
                continue;
            }

            AlgNode input = frame.remaining.next();
            if ( fingerprints.get( input ) == null ) {
                if ( input == null || fingerprints.containsKey( input ) ) {
                    return null;
                }
                fingerprints.put( input, null );
                pending.push( new Frame( input ) );
            }
        }
        return "shape-v1:" + fingerprints.get( plan );
    }


    private static HashCode fingerprint( Frame frame, Map<AlgNode, HashCode> fingerprints ) {
        String operator = frame.node.getClass().getName();
        Hasher hasher = Hashing.sha256().newHasher()
                .putInt( operator.length() )
                .putUnencodedChars( operator )
                .putInt( frame.inputs.size() );
        for ( AlgNode input : frame.inputs ) {
            hasher.putBytes( fingerprints.get( input ).asBytes() );
        }
        return hasher.hash();
    }


    private static final class Frame {

        private final AlgNode node;
        private final List<AlgNode> inputs;
        private final Iterator<AlgNode> remaining;


        private Frame( AlgNode node ) {
            this.node = node;
            this.inputs = node.getInputs();
            this.remaining = inputs == null ? Collections.emptyIterator() : inputs.iterator();
        }

    }

}
