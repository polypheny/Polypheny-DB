/*
 * Copyright 2019-2022 The Polypheny Project
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

package org.polypheny.db.view;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.polypheny.db.processing.SqlProcessorImpl;

class TriggerResolverTest {

    @Test
    void name() {
        SqlProcessorImpl sqlProcessor = new SqlProcessorImpl();
        sqlProcessor.parse("select * from emps;");
    }

    @Test
    void writeView() {
        TriggerResolver sut = new TriggerResolver();
        try {
            sut.runTriggers(null, null, null);
            Assertions.fail();
        } catch (RuntimeException e) {
            // expected
        }
    }

}