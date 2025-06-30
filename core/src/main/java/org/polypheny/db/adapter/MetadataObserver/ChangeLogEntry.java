/*
 * Copyright 2019-2025 The Polypheny Project
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

package org.polypheny.db.adapter.MetadataObserver;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.polypheny.db.adapter.MetadataObserver.PublisherManager.ChangeStatus;
import org.polypheny.db.adapter.MetadataObserver.Utils.MetaDiffUtil.DiffResult;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Value
@AllArgsConstructor
public class ChangeLogEntry {

    @JsonProperty
    String adapterName;
    @JsonProperty
    String timestamp;
    @JsonProperty
    List<String> messages;
    @JsonProperty
    ChangeStatus severity;

    public class DiffMessageUtil {

        private DiffMessageUtil() {}

        public static List<String> toMessages(DiffResult diff) {
            List<String> msgs = new ArrayList<>();

            diff.getAdded()
                    .forEach(p -> msgs.add("Added metadata " + p));

            diff.getRemoved()
                    .forEach(p -> msgs.add("Removed metadata " + p));

            diff.getChanged()
                    .forEach(p -> msgs.add("Changed metadata " + p));

            return msgs;
        }
    }


}
