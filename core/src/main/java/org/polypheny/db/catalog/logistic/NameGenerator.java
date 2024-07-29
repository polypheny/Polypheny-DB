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

package org.polypheny.db.catalog.logistic;


import java.util.concurrent.atomic.AtomicInteger;
import org.polypheny.db.config.RuntimeConfig;


public class NameGenerator {

    private final static AtomicInteger indexCounter = new AtomicInteger();
    private final static AtomicInteger foreignKeyCounter = new AtomicInteger();
    private final static AtomicInteger constraintCounter = new AtomicInteger();


    public static String generateIndexName() {
        return RuntimeConfig.GENERATED_NAME_PREFIX.getString() + "_i_" + indexCounter.getAndIncrement();
    }


    public static String generateForeignKeyName() {
        return RuntimeConfig.GENERATED_NAME_PREFIX.getString() + "_fk_" + foreignKeyCounter.getAndIncrement();
    }


    public static String generateConstraintName() {
        return RuntimeConfig.GENERATED_NAME_PREFIX.getString() + "_c_" + constraintCounter.getAndIncrement();
    }
}
