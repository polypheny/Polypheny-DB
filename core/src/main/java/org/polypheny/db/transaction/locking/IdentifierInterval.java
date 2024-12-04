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
 */

package org.polypheny.db.transaction.locking;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class IdentifierInterval implements Comparable<IdentifierInterval> {
    private long lowerBound;
    private long upperBound;

    IdentifierInterval(long lowerBound, long upperBound) {
        if (upperBound < lowerBound) {
            throw new IllegalArgumentException("Upper bound must be greater or equal than lower bound");
        }
        this.lowerBound = lowerBound;
        this.upperBound = upperBound;
    }

    @Override
    public int compareTo( IdentifierInterval other ) {
        return Long.compare( this.lowerBound, other.lowerBound );
    }

    @Override
    public String toString() {
        return "IdentifierInterval{" + lowerBound + ", " + upperBound + "}";
    }

    public long getNextIdentifier() {
        long identifier = lowerBound;
        lowerBound++;
        return identifier;
    }

    public boolean hasNextIdentifier() {
        return lowerBound < upperBound;
    }
}
