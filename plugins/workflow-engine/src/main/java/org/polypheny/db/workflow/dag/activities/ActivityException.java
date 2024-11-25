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

import lombok.Getter;

@Getter
public class ActivityException extends Exception {

    private final ActivityWrapper activity;


    public ActivityException( String message, ActivityWrapper activity ) {
        super( message );
        this.activity = activity;
    }


    @Override
    public String toString() {
        return activity.getType() + ":" + activity.getId() + ": " + super.toString();
    }


    public static class InvalidSettingException extends ActivityException {

        @Getter
        private final String settingKey;


        public InvalidSettingException( String message, ActivityWrapper activity, String settingKey ) {
            super( message, activity );
            this.settingKey = settingKey;
        }


        @Override
        public String toString() {
            return super.toString() + " (Setting Key: " + settingKey + ")";
        }

    }


    public static class InvalidInputException extends ActivityException {

        private final int index;


        public InvalidInputException( String message, ActivityWrapper activity, int index ) {
            super( message, activity );
            this.index = index;
        }


        public int getIndex() {
            return index;
        }


        @Override
        public String toString() {
            return super.toString() + " (Input Index: " + index + ")";
        }

    }

}
