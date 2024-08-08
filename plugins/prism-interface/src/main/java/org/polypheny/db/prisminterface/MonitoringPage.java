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

package org.polypheny.db.prisminterface;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Setter;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.prisminterface.statements.StatementManager;

class MonitoringPage {

    @Setter
    private ClientManager clientManager;

    private final Set<StatementManager> statementManagers = new HashSet<>();
    private final InformationPage informationPage;
    private final InformationGroup interfaceGroup;
    private final InformationTable statementsTable;
    private final InformationTable connectionsTable;

    private final InformationGroup connectionsGroup;
    private final InformationTable connectionListTable;


    MonitoringPage( String uniqueName, String description ) {
        InformationManager informationManager = InformationManager.getInstance();

        informationPage = new InformationPage( uniqueName, description )
                .fullWidth()
                .setLabel( "Interfaces" );
        interfaceGroup = new InformationGroup( informationPage, "Interface Statistics" );

        informationManager.addPage( informationPage );
        informationManager.addGroup( interfaceGroup );

        statementsTable = new InformationTable( interfaceGroup, Arrays.asList( "Attribute", "Value" ) );
        statementsTable.setOrder( 1 );
        informationManager.registerInformation( statementsTable );

        connectionsTable = new InformationTable( interfaceGroup, Arrays.asList( "Attribute", "Value" ) );
        connectionsTable.setOrder( 2 );
        informationManager.registerInformation( connectionsTable );

        connectionsGroup = new InformationGroup( informationPage, "Connections" );
        informationManager.addGroup( connectionsGroup );
        connectionListTable = new InformationTable( connectionsGroup,
                Arrays.asList( "UUID", "TX", "Auto Commit", "Default Namespace", "Features" ) );
        connectionListTable.setOrder( 3 );
        informationManager.registerInformation( connectionListTable );

        informationPage.setRefreshFunction( this::update );
    }


    private void update() {
        connectionsTable.reset();
        connectionsTable.addRow( "Open Connections", clientManager.getClientCount() );

        statementsTable.reset();
        AtomicInteger statementCount = new AtomicInteger();
        statementManagers.forEach( m -> statementCount.addAndGet( m.openStatementCount() ) );
        statementsTable.addRow( "Open Statements", statementCount.get() );

        connectionListTable.reset();
        clientManager.getClients().forEach( this::addClientEntry );
    }


    private void addClientEntry( Entry<String, PIClient> clientEntry ) {
        String uuid = clientEntry.getKey();
        PIClient client = clientEntry.getValue();
        String txId = "-";
        if ( !client.hasNoTransaction() ) {
            txId = String.valueOf( client.getOrCreateNewTransaction().getId() );
        }
        connectionListTable.addRow(
                uuid,
                txId,
                client.isAutoCommit(),
                client.getNamespace(),
                getFeatures( client )
        );
    }

    private String getFeatures(PIClient client) {
        StringBuilder stringBuilder = new StringBuilder();
        List<String> features = client.getClientConfig().getSupportedFeatures().stream().toList();
        for (int i = 0; i < features.size(); i++) {
            stringBuilder.append(features.get(i));
            if (i < features.size() - 1) {
                stringBuilder.append(", ");
            }
        }
        return stringBuilder.toString();
    }



    void addStatementManager( StatementManager statementManager ) {
        statementManagers.add( statementManager );
    }


    void removeStatementManager( StatementManager statementManager ) {
        statementManagers.remove( statementManager );
    }

    public void remove() {
        InformationManager im = InformationManager.getInstance();
        im.removeInformation( connectionsTable );
        im.removeInformation( statementsTable );
        im.removeInformation( connectionListTable );
        im.removeGroup( interfaceGroup );
        im.removePage( informationPage );
    }

}
