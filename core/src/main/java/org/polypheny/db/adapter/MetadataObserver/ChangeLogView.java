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

import java.time.Duration;
import java.time.Instant;
import java.util.List;

@Value
@AllArgsConstructor
public class ChangeLogView {

    @JsonProperty
    String adapterName;

    @JsonProperty
    String timestamp;

    @JsonProperty
    List<String> messages;

    @JsonProperty
    ChangeStatus severity;


    public static ChangeLogView from( ChangeLogEntry e ) {
        return new ChangeLogView(
                e.getAdapterName(),
                rel( e.getTimestamp() ),
                e.getMessages(),
                e.getSeverity()
        );
    }


    private static String rel( Instant then ) {
        long s = Duration.between( then, Instant.now() ).getSeconds();
        if ( s < 0 ) {
            s = 0;
        }

        if ( s < 60 ) {
            return s == 1 ? "1 second ago" : s + " seconds ago";
        }
        long m = s / 60;
        if ( m < 60 ) {
            return m == 1 ? "1 minute ago" : m + " minutes ago";
        }
        long h = m / 60;
        if ( h < 24 ) {
            return h == 1 ? "1 hour ago" : h + " hours ago";
        }
        long d = h / 24;
        if ( d < 7 ) {
            return d == 1 ? "1 day ago" : d + " days ago";
        }
        long w = d / 7;
        if ( w < 5 ) {
            return w == 1 ? "1 week ago" : w + " weeks ago";
        }
        long mo = d / 30;
        if ( mo < 12 ) {
            return mo == 1 ? "1 month ago" : mo + " months ago";
        }
        long y = d / 365;
        return y == 1 ? "1 year ago" : y + " years ago";
    }

}
