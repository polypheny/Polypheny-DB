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

package org.polypheny.db.adapter.file;


import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Method;
import java.util.List;
import org.apache.calcite.linq4j.tree.Types;
import org.polypheny.db.adapter.DataContext;
import org.polypheny.db.adapter.file.FileAlg.FileImplementor.Operation;
import org.polypheny.db.adapter.file.source.QfsSchema;


public enum FileMethod {

    EXECUTE( FileStoreSchema.class, "execute", Operation.class, Long.class, Long.class, DataContext.class, String.class, Long[].class, FileTranslatableEntity.class, List.class, List.class, Condition.class, List.class ),
    EXECUTE_MODIFY( FileStoreSchema.class, "executeModify", Operation.class, Long.class, Long.class, DataContext.class, String.class, Long[].class, FileTranslatableEntity.class, List.class, Boolean.class, List.class, Condition.class ),
    EXECUTE_QFS( QfsSchema.class, "execute", Operation.class, Long.class, Long.class, DataContext.class, String.class, Long[].class, FileTranslatableEntity.class, List.class, List.class, Condition.class, List.class );

    public final Method method;
    public static final ImmutableMap<Method, FileMethod> MAP;


    static {
        final ImmutableMap.Builder<Method, FileMethod> builder = ImmutableMap.builder();
        for ( FileMethod value : FileMethod.values() ) {
            builder.put( value.method, value );
        }
        MAP = builder.build();
    }


    FileMethod( Class<?> clazz, String methodName, Class<?>... argumentTypes ) {
        this.method = Types.lookupMethod( clazz, methodName, argumentTypes );
    }
}
