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

import java.util.List;
import java.util.Map;

public class ProtoInterfaceStatementBatch {
    private List<ProtoInterfaceStatement> statements;
    private Map<String, String> properties;

    public ProtoInterfaceStatementBatch(Map<String, String> properties) {
        this.properties = properties;
    }

    public void addStatement(ProtoInterfaceStatement protoInterfaceStatement) {
        statements.add( protoInterfaceStatement );
    }


    public void executeAll() {

    }

}
