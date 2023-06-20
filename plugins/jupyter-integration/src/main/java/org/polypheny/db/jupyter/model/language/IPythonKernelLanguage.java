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

package org.polypheny.db.jupyter.model.language;

public class IPythonKernelLanguage implements JupyterKernelLanguage {

    @Override
    public String getInitCode() {
        return "%load_ext poly";
    }


    @Override
    public String[] transformToQuery( String query, String language, String namespace, String varName, boolean expandParams ) {
        String[] queries = new String[2];
        String cleanQuery = query.strip();

        // -i: use stdin, -j: output json, -t: query is template
        queries[0] = (expandParams ? "%%poly -i -j -t load\n" : "%%poly -i -j load\n") + cleanQuery;
        queries[1] = varName + " = _";
        return queries;
    }

}
