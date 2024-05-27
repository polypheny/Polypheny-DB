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
 *
 * This file incorporates code covered by the following terms:
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.polypheny.db.config;


import java.util.Properties;
import org.polypheny.db.algebra.constant.ConformanceEnum;
import org.polypheny.db.util.Conformance;
import org.polypheny.db.util.avatica.ConnectionConfigImpl;


/**
 * Implementation of {@link PolyphenyDbConnectionConfig}.
 */
public class PolyphenyDbConnectionConfigImpl extends ConnectionConfigImpl implements PolyphenyDbConnectionConfig {

    public PolyphenyDbConnectionConfigImpl( Properties properties ) {
        super( properties );
    }


    /**
     * Returns a copy of this configuration with one property changed.
     */
    public PolyphenyDbConnectionConfigImpl set( PolyphenyDbConnectionProperty property, String value ) {
        final Properties properties1 = new Properties( properties );
        properties1.setProperty( property.camelName(), value );
        return new PolyphenyDbConnectionConfigImpl( properties1 );
    }


    @Override
    public boolean forceDecorrelate() {
        return true;
    }

    @Override
    public Conformance conformance() {
        return ConformanceEnum.DEFAULT;
    }

}
