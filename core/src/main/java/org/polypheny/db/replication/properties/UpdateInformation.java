/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.replication.properties;


import java.io.Serializable;
import lombok.Getter;


@Getter
/**
 * Contains Meta Information on an object when it was last updated. Either by a primary transaction or a replication.
 */
public class UpdateInformation implements Serializable {

    // Head of commitTimestamp of original TX
    public final long commitTimestamp;
    public final long txId;

    // Time when the modification was applied
    public final long updateTimestamp;
    public final long replicationId;

    // Number of received modifications
    public final long modifications;


    private UpdateInformation( long commitTimestamp, long txId, long updateTimestamp, long replicationId, long modifications ) {
        this.commitTimestamp = commitTimestamp;
        this.txId = txId;
        this.updateTimestamp = updateTimestamp;
        this.replicationId = replicationId;     // This is not set for eagerly updated placements
        this.modifications = modifications;
    }

    // Helper & Utility Methods


    /**
     * Creates a default Property which can be used when no further information is necessary on
     * replication for this entity.
     */
    public static UpdateInformation createEmpty() {
        return new UpdateInformation( 0, 0, 0, 0, 0 );
    }


    public static UpdateInformation create( long commitTimestamp, long txId, long updateTimestamp, long replicationId, long modifications ) {
        return new UpdateInformation( commitTimestamp, txId, updateTimestamp, replicationId, modifications );
    }
}
