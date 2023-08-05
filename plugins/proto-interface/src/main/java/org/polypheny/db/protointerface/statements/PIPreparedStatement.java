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

import org.jetbrains.annotations.NotNull;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.protointerface.PIClient;
import org.polypheny.db.protointerface.PIStatementProperties;
import org.polypheny.db.protointerface.proto.ParameterMeta;

import java.util.List;
import org.polypheny.db.protointerface.statementProcessing.StatementProcessor;

public abstract class PIPreparedStatement extends PIStatement implements Signaturizable {

    protected List<ParameterMeta> parameterMetas;

    public void setParameterMetas(List<ParameterMeta> parameterMetas) {
        this.parameterMetas = parameterMetas;
    }

    public List<ParameterMeta> getParameterMetas() {
        if (parameterMetas == null) {
            StatementProcessor.prepare(this);
        }
        return parameterMetas;
    }

    protected PIPreparedStatement(
            int id,
            @NotNull PIClient client,
            @NotNull PIStatementProperties properties,
            @NotNull QueryLanguage language,
            @NotNull LogicalNamespace namespace) {
        super(id, client, properties, language, namespace);
    }


}
