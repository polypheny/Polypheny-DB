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

package org.polypheny.db.workflow.dag.activities;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.workflow.models.ActivityModel;

public interface Activity {

    String getType();
    UUID getId();
    boolean validate( List<AlgDataType> inSchemas);
    List<AlgDataType> computeOutSchemas( List<AlgDataType> inSchemas);
    List<PortType> inPortTypes();
    List<PortType> outPortTypes();

    void execute(); // default execution method. TODO: introduce execution context to track progress, abort, inputs, outputs...

    void updateSettings( Map<String, Object> settings);
    void updateConfig( Map<String, Object> config);
    void updateRendering( Map<String, Object> rendering);

    ActivityModel toModel();

    static Activity fromModel(ActivityModel model) {
        throw new NotImplementedException();
    }

    enum PortType {
        COMMMON, // compatible with any other type
        REL,
        DOC,
        LPG
    }
}
