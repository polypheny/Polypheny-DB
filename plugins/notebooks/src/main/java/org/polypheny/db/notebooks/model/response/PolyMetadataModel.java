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

@NoArgsConstructor
public class PolyMetadataModel {

    @Getter
    @SerializedName("cell_type")
    private String cellType;

    @Getter
    private String namespace;

    @Getter
    private String language;

    @Getter
    @SerializedName("result_variable")
    private String resultVariable;

    @Getter
    @SerializedName("manual_execution")
    private Boolean manualExecution;

    @Getter
    @SerializedName("expand_params")
    private Boolean expandParams;

}
