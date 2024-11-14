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

import static org.junit.jupiter.api.Assertions.fail;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.polypheny.db.workflow.models.ActivityConfigModel;
import org.polypheny.db.workflow.models.ActivityModel;
import org.polypheny.db.workflow.models.RenderModel;

class ActivityRegistryTest {


    @Test
    public void checkRequiredConstructorsForAnnotatedActivities() {
        Set<Class<? extends Activity>> activityClasses = ActivityRegistry.findAllAnnotatedActivities();
        for (Class<? extends Activity> activityClass : activityClasses) {
            assertConstructorExists(activityClass, ActivityModel.class);
            assertConstructorExists(activityClass, UUID.class, Map.class, ActivityConfigModel.class, RenderModel.class);
        }
    }

    private static void assertConstructorExists(Class<?> activityClass, Class<?>... argumentTypes) {
        try {
            activityClass.getConstructor(argumentTypes);
        } catch (NoSuchMethodException e) {
            fail(
                    "Constructor with arguments " + java.util.Arrays.toString(argumentTypes) +
                    " is missing for: " + activityClass.getSimpleName());
        }
    }
}
