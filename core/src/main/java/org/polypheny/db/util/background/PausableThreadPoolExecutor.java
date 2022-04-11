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

package org.polypheny.db.util.background;


import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


/**
 * A light wrapper around the {@link ThreadPoolExecutor}. It allows for you to pause execution and
 * resume execution when ready.
 *
 * @author Matthew A. Johnston (warmwaffles) https://gist.github.com/warmwaffles/8534618
 */
public class PausableThreadPoolExecutor extends ThreadPoolExecutor {

    private boolean isPaused;
    private ReentrantLock lock;
    private Condition condition;


    /**
     * @param corePoolSize The size of the pool
     * @param maximumPoolSize The maximum size of the pool
     * @param keepAliveTime The amount of time you wish to keep a single task alive
     * @param unit The unit of time that the keep alive time represents
     * @param workQueue The queue that holds your tasks
     * @see {@link ThreadPoolExecutor#ThreadPoolExecutor(int, int, long, TimeUnit, BlockingQueue)}
     */
    public PausableThreadPoolExecutor( int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue ) {
        super( corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue );
        lock = new ReentrantLock();
        condition = lock.newCondition();
    }


    /**
     * @param thread The thread being executed
     * @param runnable The runnable task
     * @see {@link ThreadPoolExecutor#beforeExecute(Thread, Runnable)}
     */
    @Override
    protected void beforeExecute( Thread thread, Runnable runnable ) {
        super.beforeExecute( thread, runnable );
        lock.lock();
        try {
            while ( isPaused ) {
                condition.await();
            }
        } catch ( InterruptedException ie ) {
            thread.interrupt();
        } finally {
            lock.unlock();
        }
    }


    public boolean isRunning() {
        return !isPaused;
    }


    public boolean isPaused() {
        return isPaused;
    }


    /**
     * Pause the execution
     */
    public void pause() {
        lock.lock();
        try {
            isPaused = true;
        } finally {
            lock.unlock();
        }
    }


    /**
     * Resume pool execution
     */
    public void resume() {
        lock.lock();
        try {
            isPaused = false;
            condition.signalAll();
        } finally {
            lock.unlock();
        }
    }
}
