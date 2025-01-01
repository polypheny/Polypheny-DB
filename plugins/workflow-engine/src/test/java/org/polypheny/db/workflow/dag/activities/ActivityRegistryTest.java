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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.polypheny.db.TestHelper;
import org.polypheny.db.workflow.dag.activities.Activity.ActivityCategory;
import org.polypheny.db.workflow.dag.activities.ActivityException.InvalidSettingException;
import org.polypheny.db.workflow.dag.settings.GroupDef;
import org.polypheny.db.workflow.dag.settings.GroupDef.SubgroupDef;
import org.polypheny.db.workflow.dag.settings.IntValue;
import org.polypheny.db.workflow.dag.settings.SettingDef;
import org.polypheny.db.workflow.dag.settings.SettingDef.SettingValue;
import org.polypheny.db.workflow.dag.settings.SettingDef.Settings;
import org.polypheny.db.workflow.dag.variables.ReadableVariableStore;
import org.polypheny.db.workflow.dag.variables.VariableStore;

class ActivityRegistryTest {

    @BeforeAll
    public static void start() throws SQLException {
        //noinspection ResultOfMethodCallIgnored
        TestHelper.getInstance(); // required for access to the plugin classloader, itself a requirement for detecting all activities
    }


    @Test
    public void checkRequiredConstructorsForAnnotatedActivities() {
        Set<Class<? extends Activity>> activityClasses = ActivityRegistry.findAllAnnotatedActivities();
        assertFalse( activityClasses.isEmpty() );
        for ( Class<? extends Activity> activityClass : activityClasses ) {
            assertConstructorExists( activityClass );
        }
    }


    @Test
    public void serializationTest() {
        String serialized = ActivityRegistry.serialize();
        System.out.println( serialized );
        assertTrue( serialized.length() > 100 ); // 100 is somewhat arbitrary, just ensure it has some content
    }


    @Test
    public void checkCategoriesTest() {
        for ( ActivityDef activity : ActivityRegistry.getRegistry().values() ) {
            String activityName = activity.getActivityClass().getSimpleName();
            assertFalse( activity.hasCategory( ActivityCategory.EXTRACT ) && activity.hasCategory( ActivityCategory.LOAD ),
                    "An activity cannot extract and load data at the same time: " + activityName );
            assertTrue( activity.hasCategory(
                            ActivityCategory.EXTRACT ) ||
                            activity.hasCategory( ActivityCategory.TRANSFORM ) ||
                            activity.hasCategory( ActivityCategory.LOAD ) ||
                            activity.hasCategory( ActivityCategory.VARIABLES ),
                    "Found activity without any one of the required categories (EXTRACT, TRANSFORM, LOAD or VARIABLES): " + activityName );
        }

    }


    @Test
    public void checkActivityPortsTest() {
        for ( ActivityDef activity : ActivityRegistry.getRegistry().values() ) {
            String activityName = activity.getActivityClass().getSimpleName();
            for ( Class<?> iface : activity.getActivityClass().getInterfaces() ) {
                if ( iface.equals( Fusable.class ) ) {
                    assertTrue( activity.getOutPorts().length <= 1,
                            "A fusable activity cannot have more than 1 outPort: " + activityName );
                } else if ( iface.equals( Pipeable.class ) ) {
                    assertTrue( activity.getOutPorts().length <= 1,
                            "A pipeable activity cannot have more than 1 outPort: " + activityName );
                }
            }
        }
    }


    @Test
    public void checkGroupKeysTest() {
        for ( ActivityDef activity : ActivityRegistry.getRegistry().values() ) {
            String activityName = activity.getActivityClass().getSimpleName();
            Set<String> groupKeys = new HashSet<>();
            Map<String, Set<String>> subgroupKeysMap = new HashMap<>();
            for ( GroupDef g : activity.getGroups() ) {
                String key = g.getKey();
                assertFalse( groupKeys.contains( key ), "Found duplicate group key '" + key + "' in: " + activityName );
                groupKeys.add( key );

                Set<String> subSet = new HashSet<>();
                for ( SubgroupDef sg : g.getSubgroups() ) {
                    String subKey = sg.getKey();
                    assertFalse( subSet.contains( subKey ), "Found duplicate subgroup key '" + subKey + "' in: " + activityName );
                    subSet.add( subKey );
                }
                assertFalse( subSet.contains( GroupDef.DEFAULT_SUBGROUP ), "Cannot redefine predefined DEFAULT subgroup in: " + activityName );
                subgroupKeysMap.put( key, subSet );
            }
            // DEFAULT_GROUP and ADVANCED_GROUP redefinition is already checked with assertions within the GroupDef class.

            for ( SettingDef setting : activity.getSettings().values() ) {
                String key = setting.getGroup();
                String subKey = setting.getSubgroup();
                assertTrue( groupKeys.contains( key ), "Encountered undefined group key '" + key + "' in: " + activityName );
                assertTrue( subKey.equals( GroupDef.DEFAULT_SUBGROUP ) ||
                        subgroupKeysMap.get( key ).contains( subKey ), "Encountered undefined subgroup key '" + subKey + "' in: " + activityName );
            }
        }
    }


    @Test
    public void getDefaultSettingValuesTest() {
        for ( String key : ActivityRegistry.getRegistry().keySet() ) {
            int noSettings = ActivityRegistry.get( key ).getSettings().size();
            assertEquals( noSettings, ActivityRegistry.getSerializableDefaultSettings( key ).size() );
            System.out.println( ActivityRegistry.getSerializableDefaultSettings( key ) );
        }
    }


    @Test
    public void buildDefaultSettingValuesTest() throws InvalidSettingException {
        for ( String key : ActivityRegistry.getRegistry().keySet() ) {
            Map<String, JsonNode> defaultSettings = ActivityRegistry.getSerializableDefaultSettings( key );

            // We check whether building the default settings results in an equivalent serializable object
            Map<String, SettingValue> rebuiltSettings = ActivityRegistry.buildSettingValues( key, defaultSettings, true ).getMap();
            assertEquals( defaultSettings.size(), rebuiltSettings.size() );

            ObjectMapper mapper = new ObjectMapper();
            for ( Entry<String, SettingValue> entry : rebuiltSettings.entrySet() ) {
                JsonNode rebuiltJson = entry.getValue().toJson( mapper );
                assertEquals( defaultSettings.get( entry.getKey() ), rebuiltJson );
            }
        }
    }


    @Test
    public void intVariableResolveTest() throws InvalidSettingException {
        int newValue = 42;
        String varName = "intVariable";
        String activity = "debug";
        String settingKey = "delay";

        ObjectMapper mapper = new ObjectMapper();
        VariableStore vStore = new VariableStore();
        vStore.setVariable( varName, IntNode.valueOf( newValue ) );

        assertNotEquals( newValue, ActivityRegistry.getDefaultSettings( activity ).get( settingKey ).unwrapOrThrow( IntValue.class ).getValue() );
        Map<String, JsonNode> settings = new HashMap<>( ActivityRegistry.getSerializableDefaultSettings( activity ) );

        ObjectNode variable = mapper.createObjectNode().put( ReadableVariableStore.VARIABLE_REF_FIELD, varName );
        settings.put( settingKey, variable );
        Settings setttings = ActivityRegistry.buildSettingValues( activity, vStore.resolveVariables( settings ) );
        assertEquals( newValue, setttings.get( settingKey, IntValue.class ).getValue() );
    }


    @Test
    public void jsonPointerVariableResolveTest() throws InvalidSettingException {
        int newValue = 42;
        String varName = "intVariable";
        String activity = "debug";
        String settingKey = "delay";

        ObjectMapper mapper = new ObjectMapper();
        VariableStore vStore = new VariableStore();

        ObjectNode variableValue = mapper.createObjectNode();
        variableValue.set( "data", mapper.createArrayNode().add( 12345 ).add( newValue ) );
        vStore.setVariable( varName, variableValue );

        assertNotEquals( newValue, ActivityRegistry.getDefaultSettings( activity ).get( settingKey ).unwrapOrThrow( IntValue.class ).getValue() );
        Map<String, JsonNode> settings = new HashMap<>( ActivityRegistry.getSerializableDefaultSettings( activity ) );

        ObjectNode variable = mapper.createObjectNode().put( ReadableVariableStore.VARIABLE_REF_FIELD, varName + "/data/1" ); // reference to second object in array
        settings.put( settingKey, variable );
        Settings setttings = ActivityRegistry.buildSettingValues( activity, vStore.resolveVariables( settings ) );
        assertEquals( newValue, setttings.get( settingKey, IntValue.class ).getValue() );
    }


    private static void assertConstructorExists( Class<?> activityClass, Class<?>... argumentTypes ) {
        try {
            activityClass.getConstructor( argumentTypes );
        } catch ( NoSuchMethodException e ) {
            fail(
                    "Constructor with arguments " + java.util.Arrays.toString( argumentTypes ) +
                            " is missing for: " + activityClass.getSimpleName() );
        }
    }

}
