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

package org.polypheny.db.notebooks.model.response;

import com.google.gson.annotations.SerializedName;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * See https://jupyter-server.readthedocs.io/en/latest/developers/rest-api.html
 */
@NoArgsConstructor
public class NotebookContentModel {

    @Getter
    private String name;
    @Getter
    private String path;
    @Getter
    private String created;
    @Getter
    private String format;
    @Getter
    private String mimetype;
    @Getter
    private String size;
    @Getter
    private String writable;
    @Getter
    private String type;

    @Getter
    @SerializedName("last_modified")
    private String lastModified;

    @Getter
    private NotebookModel content;

}
