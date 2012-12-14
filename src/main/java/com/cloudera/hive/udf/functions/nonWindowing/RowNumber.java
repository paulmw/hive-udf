/**
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

package com.cloudera.hive.udf.functions.nonWindowing;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.io.LongWritable;

@Description(name = "row_number", value = "_FUNC_() - Returns an incremental row number starting from 1")
@UDFType(deterministic = false, stateful = true)
// See also UDFRowSequence in Hive
public class RowNumber extends UDF {
	  private LongWritable result = new LongWritable();

	  public RowNumber() {
		  result.set(0);
	  }

	  public LongWritable evaluate() {
		  result.set(result.get() + 1);
		  return result;
	  }
}
