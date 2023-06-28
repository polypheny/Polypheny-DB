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

package org.polypheny.db.protointerface.statements;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import org.polypheny.db.protointerface.ProtoInterfaceClient;
import org.polypheny.db.protointerface.proto.StatementBatchStatus;

public class UnparameterizedInterfaceStatementBatch extends ProtoInterfaceStatementBatch {

    List<UnparameterizedInterfaceStatement> statements;


    public UnparameterizedInterfaceStatementBatch( int batchId, ProtoInterfaceClient protoInterfaceClient, List<UnparameterizedInterfaceStatement> statements ) {
        super( batchId, protoInterfaceClient );
        this.statements = statements;
    }


    @Override
    public List<Long> execute() throws Exception {
        List<Long> updateCounts = new LinkedList<>();
        for ( UnparameterizedInterfaceStatement statement : statements ) {
            updateCounts.add( statement.execute().getScalar() );
        }
        return updateCounts;
    }

    

    @Override
    public List<ProtoInterfaceStatement> getStatements() {
        return statements.stream().map( s -> (ProtoInterfaceStatement) s ).collect( Collectors.toList() );
    }

}
