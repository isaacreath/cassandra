/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.streaming;

import org.junit.Test;

import com.codahale.metrics.Counter;
import org.apache.cassandra.db.streaming.FileStreamMetricsListener;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.metrics.StreamingMetrics;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;

public class FileStreamMetricsListenerTest
{
    @Test
    public void testFileStreamMetricsListenerShouldCountBytesTransferred()
    {
        Counter totalCounter = new Counter();
        Counter peerCounter = new Counter();
        FileStreamMetricsListener fileStreamMetricsListener = new FileStreamMetricsListener(2L, totalCounter, peerCounter);
        fileStreamMetricsListener.onStreamingBytesTransferred(1L);
        fileStreamMetricsListener.onStreamingBytesTransferred(2L);
        assertEquals("Size of the total counter should be equal to the number of bytes streamed", 2L, totalCounter.getCount());
        assertEquals("Size of the peer counter should be equal to the number of bytes stteamed", 2L, peerCounter.getCount());
    }

    @Test
    public void testFileStreamMetricsListenerShouldCheckTotalBytesStreamedOnCompletion()
    {
        Counter totalCounter = new Counter();
        Counter peerCounter = new Counter();
        FileStreamMetricsListener fileStreamMetricsListener = new FileStreamMetricsListener(2L, totalCounter, peerCounter);
        fileStreamMetricsListener.onStreamingBytesTransferred(1L);
        fileStreamMetricsListener.onStreamingBytesTransferred(2L);
        fileStreamMetricsListener.onStreamSuccessful();
    }

    @Test(expected = AssertionError.class)
    public void testFileStreamMetricsListenerShouldNotAcceptDecreasingBytesTransferred()
    {
        Counter totalCounter = new Counter();
        Counter peerCounter = new Counter();
        FileStreamMetricsListener fileStreamMetricsListener = new FileStreamMetricsListener(2L, totalCounter, peerCounter);
        fileStreamMetricsListener.onStreamingBytesTransferred(2L);
        fileStreamMetricsListener.onStreamingBytesTransferred(1L);
    }

    @Test(expected = AssertionError.class)
    public void testFileStreamMetricsListenerOnStreamSuccessfulShouldEnsureTotalBytesTransferred()
    {
        Counter totalCounter = new Counter();
        Counter peerCounter = new Counter();
        FileStreamMetricsListener fileStreamMetricsListener = new FileStreamMetricsListener(2L, totalCounter, peerCounter);
        fileStreamMetricsListener.onStreamingBytesTransferred(1L);
        fileStreamMetricsListener.onStreamSuccessful();
    }

    @Test
    public void testFileStreamMetricsListenerOnCreationShouldChooseIncomingCountersWhenDirectionIn()
    {
        InetAddressAndPort local = FBUtilities.getLocalAddressAndPort();
        FileStreamMetricsListener fileStreamMetricsListener = new FileStreamMetricsListener(1L, local, ProgressInfo.Direction.IN);
        fileStreamMetricsListener.onStreamingBytesTransferred(1L);
        assertEquals("In direction IN, total streaming metircs should be increased by cumulative amount", 1L, StreamingMetrics.totalIncomingBytes.getCount());
        assertEquals("Peer streaming metrics should be increase by cumulative amount", 1L, StreamingMetrics.get(local).incomingBytes.getCount());
    }

    @Test
    public void testFileStreamMetricsListenerOnCreationShouldChooseOutgoingCountersWhenDirectionOut()
    {
        InetAddressAndPort local = FBUtilities.getLocalAddressAndPort();
        FileStreamMetricsListener fileStreamMetricsListener = new FileStreamMetricsListener(1L, local, ProgressInfo.Direction.OUT);
        fileStreamMetricsListener.onStreamingBytesTransferred(1L);
        assertEquals("In direction OUT, total streaming metircs should be increased by cumulative amount", 1L, StreamingMetrics.totalOutgoingBytes.getCount());
        assertEquals("Peer streaming metrics should be increase by cumulative amount", 1L, StreamingMetrics.get(local).outgoingBytes.getCount());
    }
}
