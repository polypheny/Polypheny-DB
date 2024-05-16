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

package org.polypheny.db.prisminterface.statementProcessing;

import org.apache.commons.lang3.time.StopWatch;
import org.polypheny.db.PolyImplementation;
import org.polypheny.db.catalog.logistic.DataModel;
import org.polypheny.db.prisminterface.PIServiceException;
import org.polypheny.db.prisminterface.statements.PIStatement;
import org.polypheny.prism.Frame;
import org.polypheny.prism.StatementResult;

public abstract class Executor {

    protected void startOrResumeStopwatch( StopWatch stopWatch ) {
        if ( stopWatch.isSuspended() ) {
            stopWatch.resume();
            return;
        }
        if ( stopWatch.isStopped() ) {
            stopWatch.reset();
            stopWatch.start();
        }
    }


    protected void throwOnIllegalState( PIStatement piStatement ) throws PIServiceException {
        DataModel statementDataModel = piStatement.getLanguage().dataModel();

        if ( statementDataModel != getDataModel() ) {
            String message = String.format(
                    "The results of type %s returned by this statement can't be retrieved by a %s retriever.",
                    statementDataModel.name().toLowerCase(),
                    getDataModel().name().toLowerCase()
            );
            throw new PIServiceException( message, "I9000", 9000 );
        }

        if ( piStatement.getStatement() == null ) {
            throw new PIServiceException( "Statement is not linked to a polypheny statement",
                    "I9001",
                    9001
            );
        }

        PolyImplementation implementation = piStatement.getImplementation();
        if ( implementation == null ) {
            throw new PIServiceException( "Can't retrieve results form an unexecuted statement.", "I9002", 9002 );
        }
    }


    abstract DataModel getDataModel();

    abstract StatementResult executeAndGetResult( PIStatement piStatement );

    abstract StatementResult executeAndGetResult( PIStatement piStatement, int fetchSize ) throws Exception;

    abstract Frame fetch( PIStatement piStatement, int fetchSize );

}
