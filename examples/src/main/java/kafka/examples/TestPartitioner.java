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

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class TestPartitioner implements Partitioner {

	public TestPartitioner (VerifiableProperties props) {
	}

	public int partition(Object key, int a_numPartitions) {

//		int partition = 0;
//		String stringKey = (String) key;
//		int offset = stringKey.lastIndexOf('.');
//		if (offset > 0) {
//			partition = Integer.parseInt( stringKey.substring(offset+1)) % a_numPartitions;
//		}
		
		
		int partition = 0;
		String stringKey = (String) key;
		partition = Integer.parseInt(stringKey) % a_numPartitions;
		
		return partition;
	}
}