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

package org.polypheny.db.webui.models;

import lombok.Value;
import lombok.experimental.NonFinal;
import lombok.experimental.SuperBuilder;
import org.polypheny.db.catalog.logistic.NamespaceType;


@Value
@NonFinal
@SuperBuilder
public abstract class GenericResult<E> {

    /**
     * schema type of result DOCUMENT/RELATIONAL
     */
    public NamespaceType namespaceType;

    public String namespaceName;

    public E[] data;


    /**
     * Transaction id, for the websocket. It will not be serialized to gson.
     */
    public transient String xid;

    /**
     * Error message if a query failed
     */
    public String error;


    /**
     * Remove when bugs in SuperBuilder regarding generics are fixed
     */
    public static abstract class GenericResultBuilder<E, C extends GenericResult<E>, B extends GenericResultBuilder<E, C, B>> {

        protected B $fillValuesFrom( C instance ) {
            this.data = instance.data;
            this.namespaceType = instance.namespaceType;
            this.xid = instance.xid;
            this.error = instance.error;
            this.namespaceName = instance.namespaceName;

            return self();
        }

    }

}
