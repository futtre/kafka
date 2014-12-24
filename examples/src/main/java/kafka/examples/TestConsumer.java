/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.examples;

import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

public class TestConsumer implements Runnable {

	private KafkaStream<byte[], byte[]> m_stream;
	private int m_threadNumber;

	public TestConsumer(KafkaStream<byte[], byte[]> a_stream, int a_threadNumber) {

		m_threadNumber = a_threadNumber;
		m_stream = a_stream;
	}

	public void run() {

		ConsumerIterator<byte[], byte[]> it = m_stream.iterator();

		while (it.hasNext()) {

			System.out.println("Thread " + m_threadNumber + ": " + new String(it.next().message()));
		}
		System.out.println("Shutting down Thread: " + m_threadNumber);
	}
}