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

package org.polypheny.db.adaptimizer;

import java.util.HashMap;
import lombok.AccessLevel;
import lombok.Getter;
import org.polypheny.db.catalog.Catalog;
import org.polypheny.db.ddl.DdlManager;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.monitoring.core.MonitoringService;
import org.polypheny.db.monitoring.core.MonitoringServiceProvider;
import org.polypheny.db.transaction.TransactionManager;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.background.BackgroundTaskManager;

public abstract class AdaptiveOptimizerImpl implements AdaptiveOptimizer {

    // Singleton ------------------------------------------
    // Todo extend for other database systems

    private static AdaptiveOptimizerImpl INSTANCE;

    public static AdaptiveOptimizerImpl getInstance() {
        if ( INSTANCE == null ) {
            INSTANCE = new ReAdaptiveOptimizerImpl(); // Relational Optimizer
        }
        return INSTANCE;
    }

    // Information Management ------------------------------

    @Getter(AccessLevel.PROTECTED)
    protected static InformationManager informationManager;

    @Getter(AccessLevel.PROTECTED)
    private static HashMap<String, InformationPage> informationPages;

    @Getter(AccessLevel.PROTECTED)
    private static InformationTable sessionTable;

    // Manager Access  -------------------------------------

    @Getter
    protected static BackgroundTaskManager backgroundTaskManager;
    @Getter
    protected static TransactionManager transactionManager;
    @Getter
    protected static MonitoringService monitoringService;
    @Getter
    protected static Catalog catalog;
    @Getter
    protected static DdlManager ddlManager;

    // Configuration --------------------------------------

    public static void configure( TransactionManager transactManager ) {
        transactionManager = transactManager;
        monitoringService = MonitoringServiceProvider.getInstance();
        backgroundTaskManager = BackgroundTaskManager.INSTANCE;
        catalog = Catalog.getInstance();
        ddlManager = DdlManager.getInstance();
        informationManager = InformationManager.getInstance();
        Pair<InformationTable, HashMap<String, InformationPage>> pair
                = InformationUtil.configureReAdaptiveOptimizerInformation();
        sessionTable = pair.left;
        informationPages = pair.right;
    }

    // Abstract Methods  -----------------------------------


    public abstract boolean isActive( String sessionId );


    public abstract void startSession( String sessionId );


    public abstract void endSession( String sessionId );


}
