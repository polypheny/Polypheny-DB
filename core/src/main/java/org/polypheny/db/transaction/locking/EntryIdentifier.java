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

import java.util.Objects;
import lombok.Getter;
import org.polypheny.db.catalog.entity.Entity;
import org.polypheny.db.type.entity.numerical.PolyLong;

@Getter
public class EntryIdentifier {

    private final Entity entity;
    private final long entryIdentifier;



    public EntryIdentifier( Entity entity, long entryIdentifier ) {
        this.entity = entity;
        this.entryIdentifier = entryIdentifier;
    }


    @Override
    public int hashCode() {
        return Objects.hash( entryIdentifier );
    }


    @Override
    public boolean equals( Object other ) {
        if ( this == other ) {
            return true;
        }
        if ( other == null || getClass() != other.getClass() ) {
            return false;
        }
        EntryIdentifier that = (EntryIdentifier) other;
        return entryIdentifier == that.entryIdentifier;
    }


    public PolyLong getEntryIdentifierAsPolyLong() {
        return PolyLong.of( entryIdentifier );
    }

    @Override
    public String toString() {
        return "EntryIdentifier{entity=" + entity.getId() + ", entryIdentifier=" + entryIdentifier + '}';
    }

}
