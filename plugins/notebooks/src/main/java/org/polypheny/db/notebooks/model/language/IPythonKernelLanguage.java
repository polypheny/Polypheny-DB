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

package org.polypheny.db.notebooks.model.language;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * The JupyterKernelLanguage implementation for Python. It uses the
 * <a href="https://pypi.org/project/ipython-polypheny/">IPython Polypheny Extension</a>.
 */
public class IPythonKernelLanguage implements JupyterKernelLanguage {

    @Override
    public List<String> getInitCode() {
        return Collections.singletonList( "%load_ext poly" );
    }


    @Override
    public List<JupyterQueryPart> transformToQuery( String query, String language, String namespace, String varName, boolean expandParams ) {
        List<JupyterQueryPart> queries = new LinkedList<>();
        String cleanQuery = query.strip();

        // -i: use stdin, -j: output json, -t: query is template
        queries.add( new JupyterQueryPart(
                (expandParams ? "%%poly -i -j -t load\n" : "%%poly -i -j load\n") + cleanQuery,
                false, true ) );
        queries.add( new JupyterQueryPart( varName + " = _", true, false ) );
        return queries;
    }


    @Override
    public List<String> getExportedInitCode() {
        return Collections.singletonList( "%load_ext poly\n%poly db: http://localhost:13137" );
    }


    @Override
    public List<String> exportedQuery( String query, String language, String namespace, String varName, boolean expandParams ) {
        List<String> code = new LinkedList<>();
        String cleanQuery = query.strip();
        code.add( (expandParams ? "%%poly -t " : "%%poly ") + language + " " + namespace + "\n" + cleanQuery );
        code.add( varName + " = _" );
        return code;
    }

}
