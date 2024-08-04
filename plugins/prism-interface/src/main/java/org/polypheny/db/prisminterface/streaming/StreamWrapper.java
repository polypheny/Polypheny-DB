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

package org.polypheny.db.prisminterface.streaming;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.io.StringWriter;
import lombok.Getter;
import org.polypheny.prism.StreamFrame.DataCase;

@Getter
public class StreamWrapper {


    private final DataCase streamType;
    private final PipedInputStream binaryInputStream;
    private final PipedOutputStream binaryOutputStream;
    private final StringWriter stringStream;


    public StreamWrapper( PipedOutputStream pipedOutputStream ) throws IOException {
        this.streamType = DataCase.BINARY;
        this.binaryInputStream = new PipedInputStream(pipedOutputStream);
        this.binaryOutputStream = pipedOutputStream;
        this.stringStream = null;
    }


    public StreamWrapper( StringWriter stringWriter ) {
        this.streamType = DataCase.STRING;
        this.stringStream = stringWriter;
        this.binaryInputStream = null;
        this.binaryOutputStream = null;
    }

}

