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

package org.polypheny.db.workflow.dag.activities.impl;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.commons.lang3.NotImplementedException;
import org.polypheny.db.algebra.type.AlgDataType;
import org.polypheny.db.workflow.dag.activities.AbstractActivity;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.ActivityException;
import org.polypheny.db.workflow.dag.annotations.ActivityDefinition;
import org.polypheny.db.workflow.dag.annotations.DefaultGroup;
import org.polypheny.db.workflow.dag.annotations.Group;
import org.polypheny.db.workflow.dag.annotations.Group.Subgroup;
import org.polypheny.db.workflow.dag.annotations.IntSetting;
import org.polypheny.db.workflow.dag.annotations.StringSetting;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue;
import org.polypheny.db.workflow.engine.execution.ExecutionContext;
import org.polypheny.db.workflow.engine.storage.CheckpointReader;
import org.polypheny.db.workflow.models.ActivityConfigModel;
import org.polypheny.db.workflow.models.ActivityModel;
import org.polypheny.db.workflow.models.RenderModel;

@ActivityDefinition(type = "identity", displayName = "Identity", categories = { ActivityCategory.TRANSFORM })

@DefaultGroup(subgroups = {
        @Subgroup(key = "a", displayName = "Sub1"),
        @Subgroup(key = "b", displayName = "Sub2")
})
@IntSetting(key = "I1", displayName = "FIRST", defaultValue = 2)
@StringSetting(key = "S1", displayName = "SECOND", subGroup = "a")

@Group(key = "group1", displayName = "Group 1")
@IntSetting(key = "I2", displayName = "FIRST", defaultValue = 0, isList = true)
@StringSetting(key = "S2", displayName = "THIRD", defaultValue = "test", isList = true)
public class IdentityActivity extends AbstractActivity {


    public IdentityActivity( UUID id, Map<String, JsonNode> settings, ActivityConfigModel config, RenderModel rendering ) {
        super( id, settings, config, rendering );
    }


    public IdentityActivity( ActivityModel model ) {
        super( model.getId(), model.getSettings(), model.getConfig(), model.getRendering() );
    }


    @Override
    public Optional<AlgDataType>[] previewOutTypes( Optional<AlgDataType>[] inTypes, Map<String, Optional<SettingValue>> settings ) throws ActivityException {
        throw new NotImplementedException();
    }


    @Override
    public void execute( CheckpointReader[] inputs, ExecutionContext ctx ) throws Exception {
        throw new NotImplementedException();
    }


    @Override
    public void reset() {
        throw new NotImplementedException();
    }

}
