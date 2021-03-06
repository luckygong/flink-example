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

package gelly.examples.data;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.types.NullValue;

import java.util.LinkedList;
import java.util.List;

/**
 * Provides the default data sets used for the connected components example program.
 * If no parameters are given to the program, the default data sets are used.
 */
public class ConnectedComponentsDefaultData {

	public static final Integer MAX_ITERATIONS = 4;

	public static final String EDGES = "1	2\n" + "2	3\n" + "2	4\n" + "3	4";

	public static final Object[][] DEFAULT_EDGES = new Object[][] {
		new Object[]{1L, 2L},
		new Object[]{2L, 3L},
		new Object[]{2L, 4L},
		new Object[]{3L, 4L}
	};

	public static DataSet<Edge<Long, NullValue>> getDefaultEdgeDataSet(ExecutionEnvironment env) {
		List<Edge<Long, NullValue>> edgeList = new LinkedList<Edge<Long, NullValue>>();
		for (Object[] edge : DEFAULT_EDGES) {
			edgeList.add(new Edge<Long, NullValue>((Long) edge[0], (Long) edge[1], NullValue.getInstance()));
		}
		return env.fromCollection(edgeList);
	}

	public static final String VERTICES_WITH_MIN_ID = "1,1\n" + "2,1\n" + "3,1\n" + "4,1";

	private ConnectedComponentsDefaultData() {}
}
