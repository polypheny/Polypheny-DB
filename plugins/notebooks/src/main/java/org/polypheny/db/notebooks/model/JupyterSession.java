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

package org.polypheny.db.notebooks.model;

import lombok.Getter;
import lombok.Setter;

/**
 * Represents a model of an open session in the jupyter server.
 */
public class JupyterSession {

    @Getter
    @Setter
    private JupyterKernel kernel;
    @Getter
    private final String sessionId;
    @Getter
    @Setter
    private String name, path;


    public JupyterSession( String sessionId, String name, String path, JupyterKernel kernel ) {
        this.sessionId = sessionId;
        this.name = name;
        this.path = path;
        this.kernel = kernel;
    }

}
