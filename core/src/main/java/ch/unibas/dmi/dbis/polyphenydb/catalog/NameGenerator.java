/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 Databases and Information Systems Research Group, University of Basel, Switzerland
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 */

package ch.unibas.dmi.dbis.polyphenydb.catalog;


import ch.unibas.dmi.dbis.polyphenydb.config.RuntimeConfig;
import java.util.concurrent.atomic.AtomicInteger;


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
