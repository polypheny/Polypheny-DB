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

import java.util.List;
import lombok.Setter;
import org.jetbrains.annotations.NotNull;
import org.polypheny.db.catalog.entity.logical.LogicalNamespace;
import org.polypheny.db.languages.QueryLanguage;
import org.polypheny.db.prisminterface.PIClient;
import org.polypheny.db.prisminterface.statementProcessing.StatementProcessor;
import org.polypheny.prism.ParameterMeta;

@Setter
public abstract class PIPreparedStatement extends PIStatement implements Signaturizable {

    protected List<ParameterMeta> parameterMetas;


    public List<ParameterMeta> getParameterMetas() {
        if ( parameterMetas == null ) {
            StatementProcessor.prepare( this );
        }
        return parameterMetas;
    }


    protected PIPreparedStatement(
            int id,
            @NotNull PIClient client,
            @NotNull QueryLanguage language,
            @NotNull LogicalNamespace namespace ) {
        super( id, client, language, namespace );
    }


}
