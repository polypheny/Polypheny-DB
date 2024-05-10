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

package org.polypheny.db.prisminterface.statements;

import java.util.LinkedList;
import java.util.List;
import lombok.Getter;
import org.polypheny.db.prisminterface.PIClient;
import org.polypheny.db.prisminterface.utils.PropertyUtils;

@Getter
public class PIUnparameterizedStatementBatch {

    List<PIUnparameterizedStatement> statements;
    PIClient prismInterfaceClient;
    int batchId;


    public PIUnparameterizedStatementBatch( int batchId, PIClient prismInterfaceClient, List<PIUnparameterizedStatement> statements ) {
        this.statements = statements;
        this.prismInterfaceClient = prismInterfaceClient;
        this.batchId = batchId;
    }


    public List<Long> executeBatch() {
        int fetchSize = PropertyUtils.DEFAULT_FETCH_SIZE;
        List<Long> updateCounts = new LinkedList<>();
        for ( PIUnparameterizedStatement statement : statements ) {
            updateCounts.add( statement.execute( fetchSize ).getScalar() );
        }
        return updateCounts;
    }

}
