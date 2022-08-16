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
import java.util.List;
import org.polypheny.db.adaptimizer.models.Classifier;
import org.polypheny.db.adaptimizer.rndqueries.QuerySupplier;
import org.polypheny.db.adaptimizer.rndqueries.QueryUtil;
import org.polypheny.db.adaptimizer.rndqueries.RelQueryGenerator;
import org.polypheny.db.adaptimizer.rndschema.DefaultTestEnvironment;
import org.polypheny.db.adaptimizer.rnddata.DataUtil;
import org.polypheny.db.adaptimizer.rndschema.SchemaUtil;
import org.polypheny.db.adaptimizer.sessions.CoverageSession;
import org.polypheny.db.adaptimizer.sessions.SessionUtil;
import org.polypheny.db.adaptimizer.sessions.TreeMeasureSession;
import org.polypheny.db.catalog.exceptions.UnknownColumnException;
import org.polypheny.db.information.Information;
import org.polypheny.db.information.InformationAction;
import org.polypheny.db.information.InformationGroup;
import org.polypheny.db.information.InformationManager;
import org.polypheny.db.information.InformationPage;
import org.polypheny.db.information.InformationTable;
import org.polypheny.db.information.InformationText;
import org.polypheny.db.util.Pair;
import org.polypheny.db.util.PolyphenyHomeDirManager;

public abstract class InformationUtil {
    // Constants -----------------------------------------

    private static final String INFO_PAGE_LABEL = "Adaptive Cost Optimization";


    public static Pair<InformationTable, HashMap<String, InformationPage>>  configureReAdaptiveOptimizerInformation() {

        configureFileSystem();

        InformationManager informationManager = AdaptiveOptimizerImpl.getInformationManager();
        HashMap<String, InformationPage> informationPages = new HashMap<>();

        // Information
        InformationGroup group;

        // Random Schema Generation
        addInformationPage( informationManager, informationPages, "schema", "Random Schema Generation", "" );
        group = addInformationGroup( informationManager, informationPages, "schema", "Default");
        addInformationElements( informationManager, group, List.of(
                new InformationText( group, "Generating Environments:" ),
                new InformationAction( group, "Default", parameters -> DefaultTestEnvironment.generate()  )
        ) );
        group = addInformationGroup( informationManager, informationPages, "schema", "Random");
        addInformationElements( informationManager, group, List.of(
                new InformationText( group, "Generating Environments Randomly:" ),
                new InformationAction( group, "Add DataStores", parameters -> SchemaUtil.addRndSchemaTestDataStores() ),
                new InformationAction( group, "Generate", SchemaUtil::generateSchema )
                        .withParameters( "seed", "refP" )
        ) );

        // Random Data Generation
        addInformationPage( informationManager, informationPages, "data", "Random Data Generation", "" );
        group = addInformationGroup( informationManager, informationPages, "data", "Actions");
        addInformationElements( informationManager, group, List.of(
                new InformationText( group, "Generate Data for Schemas" ),
                new InformationAction( group, "Default", parameters -> generateDefaultData() )
        ) );

        // Random Query Generation
        addInformationPage( informationManager, informationPages, "query", "Random Query Generation", "" );
        group = addInformationGroup( informationManager, informationPages, "query", "Actions");
        addInformationElements( informationManager, group, List.of(
                new InformationText( group, "Generate Queries" ),
                new InformationAction( group, "Default", parameters -> "?" ),
                new InformationAction( group, "CoverageTest", parameters -> testCoverage() ),
                new InformationAction( group, "ComplexityTest", parameters -> testComplexity() )
        ) );

        // Optimization Sessions
        addInformationPage( informationManager, informationPages, "session", "Adaptive Optimizer Sessions", "" );
        group = addInformationGroup( informationManager, informationPages, "session", "Actions");
        addInformationElements( informationManager, group, List.of(
                new InformationText( group, "Sessions" ),
                new InformationAction( group, "Default", parameters -> {
                    AdaptiveOptimizerImpl reAdaptiveOptimizer = AdaptiveOptimizerImpl.getInstance();
                    String sessionId = reAdaptiveOptimizer.createSession();
                    reAdaptiveOptimizer.startSession( sessionId );
                    return "Session Started.";
                } ),
                new InformationAction( group, "Custom", parameters -> {
                    AdaptiveOptimizerImpl reAdaptiveOptimizer = AdaptiveOptimizerImpl.getInstance();
                    String sessionId;
                    try {
                        sessionId = reAdaptiveOptimizer.createSession( parameters );
                    } catch ( Exception e ) {
                        return "Failed: " + e.getMessage();
                    }
                    reAdaptiveOptimizer.startSession( sessionId );
                    return "Session Started.";
                } ).withParameters(
                        "Schema",
                        "Tables",
                        "Nr. of Queries",
                        "Tree Height",
                        "Seed",
                        "Unary Probability (Float)",
                        "Union Frequency (Int)",
                        "Minus Frequency (Int)",
                        "Intersect Frequency (Int)",
                        "Join Frequency (Int)",
                        "Project Frequency (Int)",
                        "Project Frequency (Int)",
                        "Sort Frequency (Int)",
                        "Filter Frequency (Int)"
                )
        ) );

        InformationTable sessionTable = new InformationTable( group, List.of(
                "SID",
                "Seed",
                "Time",
                "#Err",
                "#Exc",
                "#Try",
                "#Ssw",
                "BM-0",
                "BM-1",
                "Success"
        ) );
        addInformationElements( informationManager, group, List.of( sessionTable ) );


        group = addInformationGroup( informationManager, informationPages, "session", "Update");
        addInformationElements( informationManager, group, List.of(
                new InformationText( group, "Update the Classifier" ),
                new InformationAction( group, "Update", parameters -> {
                    Classifier.update();
                    return "Updated";
                } )
        ) );

        return new Pair<>( sessionTable, informationPages );
    }

    private static void addInformationElements( InformationManager informationManager, InformationGroup group, List<Information> infos ) {
        infos.forEach( group::addInformation );
        infos.forEach( informationManager::registerInformation );
    }

    private static InformationGroup addInformationGroup( InformationManager informationManager, HashMap<String, InformationPage> informationPages, String key, String name ) {
        InformationGroup group = new InformationGroup( informationPages.get( key ), name );
        informationManager.addGroup( group );
        return group;
    }

    private static void addInformationPage( InformationManager informationManager, HashMap<String, InformationPage> informationPages, String key, String title, String description ) {
        informationPages.put( key, new InformationPage( title, description ).setLabel( INFO_PAGE_LABEL ) );
        InformationPage page = informationPages.get( key );
        informationManager.addPage( page );
    }

    private static void configureFileSystem() {
        PolyphenyHomeDirManager fileSystemManager = PolyphenyHomeDirManager.getInstance();
        fileSystemManager.registerNewFolder( "re-adaptive-optimizer" );
    }

    private static String generateDefaultData() {
        try {
            DataUtil.generateDataForDefaultEnvironment();
        } catch ( UnknownColumnException e ) {
            e.printStackTrace();
            return "Failed";
        }
        return "Success";
    }

    private static String testCoverage() {
        CoverageSession coverageSession = new CoverageSession( new QuerySupplier( RelQueryGenerator.from( SessionUtil.getCoverageMeasureTemplate() ) ) );
        coverageSession.run();
        return "Success";
    }

    private static String testComplexity() {
        TreeMeasureSession coverageSession = new TreeMeasureSession( new QuerySupplier( RelQueryGenerator.from( SessionUtil.getCoverageMeasureTemplate() ) ) );
        coverageSession.run();
        return "Success";
    }

}
