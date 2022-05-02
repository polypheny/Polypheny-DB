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


import org.polypheny.db.adaptimizer.randomschema.DefaultTestEnvironment;

/**
 * The {@link ReAdaptiveOptimizerImpl} is the base class of the Adaptive Optimizer
 * and exposes functions for the UI.
 */
public class ReAdaptiveOptimizerImpl extends ReAdaptiveOptimizer {


    public ReAdaptiveOptimizerImpl() {

        // Pass Transaction Manager
        DefaultTestEnvironment.setTransactionManager( getTransactionManager() );
        AdaptimizerInformation.setTransactionManager( getTransactionManager() );

        // Add Information Page
        AdaptimizerInformation.addInformationPage();
        AdaptimizerInformation.addInformationGroupForTestDataGeneration();
        AdaptimizerInformation.addInformationGroupForGeneratingTestEnvironment();
        AdaptimizerInformation.addInformationGroupForRandomTreeGeneration();

    }


    /**
     * Runs an optimization session.
     */
    @Override
    public void run() {

    }

}
