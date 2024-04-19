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

package org.polypheny.db.protointerface.statementProcessing;

import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.protointerface.statements.PIStatement;

public abstract class StatementImplementer {

    protected final String ORIGIN = "Proto-Interface";


    protected boolean hasInvalidLanguage( PIStatement piStatement ) {
        return piStatement.getLanguage() != getLanguage();
    }


    abstract QueryLanguage getLanguage();

    abstract void implement( PIStatement piStatement );

}
