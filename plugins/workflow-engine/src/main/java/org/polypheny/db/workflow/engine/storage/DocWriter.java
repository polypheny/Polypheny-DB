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

package org.polypheny.db.workflow.engine.storage;

import java.util.List;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.catalog.entity.logical.LogicalCollection;
import org.polypheny.db.transaction.Transaction;
import org.polypheny.db.type.entity.PolyValue;
import org.polypheny.db.type.entity.document.PolyDocument;

public class DocWriter extends CheckpointWriter {

    public DocWriter( LogicalCollection collection, Transaction transaction ) {
        super( collection, transaction );
    }


    public void write( PolyDocument document ) {
        throw new NotImplementedException();
    }


    @Override
    public void write( List<PolyValue> tuple ) {
        throw new NotImplementedException();
    }


    @Override
    public void close() throws Exception {
        throw new NotImplementedException();
    }


    private LogicalCollection getCollection() {
        return (LogicalCollection) entity;
    }

}