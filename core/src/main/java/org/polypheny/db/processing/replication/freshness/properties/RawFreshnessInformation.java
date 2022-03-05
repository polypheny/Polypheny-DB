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

package org.polypheny.db.processing.replication.freshness.properties;


import lombok.Getter;
import org.polypheny.db.nodes.Identifier;
import org.polypheny.db.nodes.Node;


@Getter

public class RawFreshnessInformation {

    // Considered as the lower bound
    public Node toleratedFreshness;
    public Identifier rawEvaluationType;
    public Identifier unit;


    public void RawPartitionInformation( Node toleratedFreshness, Identifier rawEvaluationType, Identifier unit ) {
        this.toleratedFreshness = toleratedFreshness;
        this.rawEvaluationType = rawEvaluationType;
        this.unit = unit;
    }

}
