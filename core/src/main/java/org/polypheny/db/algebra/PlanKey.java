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

package org.polypheny.db.algebra;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Immutable structural identity of a plan or a part of a plan. */
public final class PlanKey {

    private final List<Object> components;
    private final int hash;


    private PlanKey( List<Object> components ) {
        this.components = components;
        this.hash = components.hashCode();
    }


    /** Returns an empty result if a component cannot be represented safely. */
    public static Optional<PlanKey> of( Object... components ) {
        List<Object> snapshot = PlanKeyComponents.snapshot( components == null ? Collections.singletonList( null ) : Arrays.asList( components ) );
        return snapshot == null ? Optional.empty() : Optional.of( new PlanKey( snapshot ) );
    }


    @Override
    public boolean equals( Object other ) {
        return this == other || other instanceof PlanKey key && hash == key.hash && components.equals( key.components );
    }


    @Override
    public int hashCode() {
        return hash;
    }

}
