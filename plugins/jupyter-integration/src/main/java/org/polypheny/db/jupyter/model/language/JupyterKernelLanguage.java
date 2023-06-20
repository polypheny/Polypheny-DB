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

/**
 * Represents a translator for a given Jupyter Kernel Implementation.
 * Its purpose is to transform a query into kernel-language specific code, which can be sent and executed by the kernel.
 * Without a corresponding JupyterKernelLanguage implementation, Polypheny cells are not supported by a kernel.
 */
public interface JupyterKernelLanguage {

    /**
     * Returns code that can be used to initialize the
     * connection to polypheny. It is executed exactly once per running kernel and is guaranteed to take place before
     * any query is sent.
     */
    String getInitCode();

    /**
     * Transforms the specified query into code (possibly split up in several cells) to be executed by the kernel.
     * The effect of this code must be that
     * (1) the kernel sends an input request on stdin with the query as prompt.
     * If expandParams is true, the kernel must first replace ${variable} by the variable values.
     * (2) The kernel can expect that the input sent via stdin is the resultSet of the query in JSON format.
     * This result can then be optionally wrapped into some object by the kernel and stored in a variable called varName.
     * (3) Finally, the resultSet must be sent back as JSON display_data.
     *
     * @param query the query to be used
     * @param language the query language to be used
     * @param namespace the namespace to be used
     * @param varName the name of the variable the result of the query should be assigned to
     * @param expandParams whether the pattern ${variable} should be replaced with the value of the variable
     * @return the query transformed into code to be executed by the kernel
     */
    String[] transformToQuery( String query, String language, String namespace, String varName, boolean expandParams );

}
