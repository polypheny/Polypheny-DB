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

package org.polypheny.db.mqtt;

import com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish;
import lombok.extern.slf4j.Slf4j;

import java.io.UnsupportedEncodingException;
import java.nio.Buffer;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.NoSuchElementException;

@Slf4j
public class StreamProcessing {
    private Byte message;
    // Message printed: Optional[java.nio.HeapByteBufferR[pos=0 lim=5 cap=5]].
    //new String( new byte[]{info}, "US-ASCII" )
    public static String processMsg(Mqtt3Publish subMsg) {
        String msg = toString(subMsg);
        String info = extractInfo(msg);
        if (validateMsg(msg)) {
            return msg;
        } else {
            log.error( "Message is not valid!" );
            return null;
        }
    }

    private static String toString(Mqtt3Publish subMsg) {
        return new String(subMsg.getPayloadAsBytes(), Charset.defaultCharset());
    }

    private static boolean validateMsg(String msg) {
        //TODO: Implement
        return true;
    }

    private static String extractInfo(String msg) {
        //TODO: extract the needed Info only -> based on topic attribut on right side!!};
        return msg;
    }


}
