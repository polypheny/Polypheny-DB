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

<<<<<<<< HEAD:plugins/notebooks/src/main/java/org/polypheny/db/notebooks/model/JupyterKernelSpec.java
package org.polypheny.db.notebooks.model;

/**
 * Represents a model of an available kernel specification in the jupyter server.
 */
public class JupyterKernelSpec {

    public final String name, displayName, language;


    public JupyterKernelSpec( String name, String displayName, String language ) {
        this.name = name;
        this.displayName = displayName;
        this.language = language;
========
package org.polypheny.db.protointerface;

import java.io.Serializable;

public class PIServiceException extends RuntimeException implements Serializable {
    public PIServiceException(String message) {
        super(message);
>>>>>>>> 2944a6bca (add gRPC server and service for proto-interface):plugins/proto-interface/src/main/java/org/polypheny/db/protointerface/ProtoInterfaceServiceException.java
    }
}
