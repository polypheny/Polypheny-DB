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

package org.polypheny.db.protointerface;

import lombok.Getter;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.protointerface.proto.ConnectionProperties;

public class PIClientProperties {
    public PIClientProperties(ConnectionProperties clientProperties) {
        update(clientProperties);
    }

    private PIClientProperties() {
    }


    public static PIClientProperties getDefaultInstance() {
        PIClientProperties defaults = new PIClientProperties();
        defaults.isAutoCommit = true;
        defaults.isReadOnly = false; // currently not in use
        defaults.networkTimeout = 0;  // currently not in use (responsibility of client)
        defaults.isolation = ConnectionProperties.Isolation.COMMITTED; // currently not in use
        defaults.namespaceName = Catalog.defaultNamespaceName;
        return defaults;
    }


    @Getter
    private boolean isAutoCommit;
    @Getter
    private boolean isReadOnly; // currently not in use
    @Getter
    private int networkTimeout; // currently not in use
    @Getter
    private ConnectionProperties.Isolation isolation; // currently not in use
    @Getter
    private String namespaceName;

    public void updateNamespaceName(String namespaceName) {
        this.namespaceName = namespaceName;
    }

    public boolean haveNamespaceName() {
        return namespaceName != null;
    }

    public void update(ConnectionProperties clientProperties) {
        this.isAutoCommit = clientProperties.getIsAutoCommit();
        this.isReadOnly = clientProperties.getIsReadOnly();
        this.isolation = clientProperties.getIsolation();
        this.networkTimeout = clientProperties.getNetworkTimeout();
        this.namespaceName = clientProperties.hasNamespaceName() ? clientProperties.getNamespaceName() : null;
    }
}
