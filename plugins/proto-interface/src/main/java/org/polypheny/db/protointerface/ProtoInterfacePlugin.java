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

import com.google.common.collect.ImmutableList;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.iface.QueryInterface;
import org.polypheny.db.iface.QueryInterfaceManager;
import org.polypheny.db.plugins.PluginContext;
import org.polypheny.db.plugins.PolyPlugin;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.Util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ProtoInterfacePlugin extends PolyPlugin {
    protected ProtoInterfacePlugin(PluginContext context) {
        super(context);
    }

    @Override
    public void start() {
        // Add HTTP interface
        Map<String, String> settings = new HashMap<>();
        settings.put("port", "13137");
        QueryInterfaceManager.addInterfaceType("proto-interface", ProtoInterface.class, settings);
    }

    public void stop() {
        QueryInterfaceManager.removeInterfaceType(ProtoInterface.class);
    }

    private static class ProtoInterface extends QueryInterface {
        public static final String INTERFACE_NAME = "proto-interface";
        public static final List<QueryInterfaceSetting> AVAILABLE_SETTINGS = ImmutableList.of(
                new QueryInterfaceSettingInteger("port", false, true, false, 20000)
        );
        private final String uniqueName;
        private final int port;
        private TransactionManager transactionManager;
        private Authenticator authenticator;
        private ProtoInterfaceServer protoInterfaceServer;

        public ProtoInterface(TransactionManager transactionManager, Authenticator authenticator, long queryInterfaceId, String uniqueName, Map<String, String> settings, boolean supportsDml, boolean supportsDdl) {
            super(transactionManager, authenticator, queryInterfaceId, uniqueName, settings, supportsDml, supportsDdl);
            this.uniqueName = uniqueName;
            this.authenticator = authenticator;
            this.transactionManager = transactionManager;
            this.port = Integer.parseInt(settings.get("port"));
            if (!Util.checkIfPortIsAvailable(port)) {
                // Port is already in use
                throw new RuntimeException("Unable to start " + INTERFACE_NAME + " on port " + port + "! The port is already in use.");
            }
        }

        @Override

        public List<QueryInterfaceSetting> getAvailableSettings() {
            return null;
        }

        @Override
        public void shutdown() {
            try {
                protoInterfaceServer.shutdown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public String getInterfaceType() {
            return INTERFACE_NAME;
        }

        @Override
        protected void reloadSettings(List<String> updatedSettings) {
        }

        @Override
        public void languageChange() {
        }

        @Override
        public void run() {
            ClientManager clientManager = new ClientManager(authenticator, transactionManager);
            ProtoInterfaceService protoInterfaceService = new ProtoInterfaceService(clientManager);
            protoInterfaceServer = new ProtoInterfaceServer(port, protoInterfaceService, clientManager);
            try {
                protoInterfaceServer.blockUntilShutdown();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
import lombok.extern.slf4j.Slf4j;
import org.pf4j.Extension;
import org.polypheny.db.iface.Authenticator;
import org.polypheny.db.iface.QueryInterface;
import org.polypheny.db.plugins.PluginContext;
import org.polypheny.db.plugins.PolyPlugin;
import org.polypheny.db.transaction.TransactionManager;

import java.util.List;
import java.util.Map;

public class ProtoInterfacePlugin extends PolyPlugin {
    protected ProtoInterfacePlugin(PluginContext context) {
        super(context);
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Slf4j
    @Extension
    public static class ProtoInterface extends QueryInterface {


        public ProtoInterface(TransactionManager transactionManager, Authenticator authenticator, long queryInterfaceId, String uniqueName, Map<String, String> settings, boolean supportsDml, boolean supportsDdl) {
            super(transactionManager, authenticator, queryInterfaceId, uniqueName, settings, supportsDml, supportsDdl);
        }

        @Override
        public List<QueryInterfaceSetting> getAvailableSettings() {
            return null;
        }

        @Override
        public void shutdown() {

        }

        @Override
        public String getInterfaceType() {
            return null;
        }

        @Override
        protected void reloadSettings(List<String> updatedSettings) {

        }

        @Override
        public void languageChange() {

        }

        @Override
        public void run() {

        }
    }
}
