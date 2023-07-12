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
import org.polypheny.db.protointerface.proto.ConnectionProperties;

public class InterfaceClientProperties {
    public InterfaceClientProperties(ConnectionProperties clientProperties) {
        update(clientProperties);
    }

    @Getter
    private String username;
    @Getter
    private String password;
    @Getter
    private boolean isAutoCommit;
    @Getter
    private boolean isReadOnly;
    @Getter
    private ConnectionProperties.Holdability resultSetHoldability;
    @Getter
    private int networkTimeout;
    @Getter
    private ConnectionProperties.Isolation transactionIsolation;
    @Getter
    private String namespaceName;

    public void updateNamespaceName(String namespaceName) {
        this.namespaceName = namespaceName;
    }

    public boolean haveCredentials() {
        return username != null && password != null;
    }

    public boolean haveNamespaceName() {
        return namespaceName != null;
    }

    public void update(ConnectionProperties clientProperties) {
        this.username = clientProperties.hasUsername() ? clientProperties.getUsername() : null;
        this.password = clientProperties.hasPassword() ? clientProperties.getPassword() : null;
        this.isAutoCommit = clientProperties.getIsAutoCommit();
        this.isReadOnly = clientProperties.getIsReadOnly();
        this.resultSetHoldability = clientProperties.getHoldability();
        this.transactionIsolation = clientProperties.getIsolation();
        this.networkTimeout = clientProperties.getNetworkTimeout();
        this.namespaceName = clientProperties.hasNamespaceName() ? clientProperties.getNamespaceName() : null;
    }
}
