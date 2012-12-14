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

package com.cloudera.hive.udf.functions.windowing;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * This UDF provides a dense_rank() function.
 */
@Description(name = "rank", value = "_FUNC_(value, partition columns ...) - Returns the dense_rank of a value within a partitioned, sorted window.")
@UDFType(deterministic = false, stateful = true)
public class DenseRank extends GenericUDF {

	private long counter;
	private Object[] previousKey;
	private ObjectInspector[] ois;

	@Override
	public Object evaluate(DeferredObject[] currentKey) throws HiveException {
		if (!sameGroup(currentKey)) {
			this.counter = 0;
			copyToPreviousKey(currentKey);
			return new Long(++this.counter);
		} else {
			// Same group. Same value as well?
			if (!sameValue(currentKey)) {
				copyToPreviousKey(currentKey);
				return new Long(++this.counter);
			} else {
				copyToPreviousKey(currentKey);
				return new Long(this.counter);
			}
		}
	}

	@Override
	public String getDisplayString(String[] currentKey) {
		return "DenseRank";
	}

	@Override
	public ObjectInspector initialize(ObjectInspector[] arg0) throws UDFArgumentException {
		ois=arg0;
		return PrimitiveObjectInspectorFactory.javaLongObjectInspector;
	}

	/**
	 * This will help us copy objects from currrentKey to previousKeyHolder.
	 *
	 * @param currentKey
	 * @throws HiveException
	 */
	private void copyToPreviousKey(DeferredObject[] currentKey) throws HiveException {
		if (currentKey != null) {
			previousKey = new Object[currentKey.length];
			for (int index = 0; index < currentKey.length; index++) {   
				previousKey[index]= ObjectInspectorUtils
						.copyToStandardObject(currentKey[index].get(),this.ois[index]);

			}
		}   
	}

	/**
	 * This will help us compare the currentKey and previousKey objects.
	 *
	 * @param currentKey
	 * @return - true if both are same else false
	 * @throws HiveException
	 */
	private boolean sameGroup(DeferredObject[] currentKey) throws HiveException {
		boolean status = false;

		//if both are null then we can classify as same
		if (currentKey == null && previousKey == null) {
			status = true;
		}

		//if both are not null and there legnth as well as
		//individual elements are same then we can classify as same
		if (currentKey != null && previousKey != null && currentKey.length == previousKey.length) {
			for (int index = 1; index < currentKey.length; index++) {

				if (ObjectInspectorUtils.compare(currentKey[index].get(), this.ois[index],
						previousKey[index],
						ObjectInspectorFactory.getReflectionObjectInspector(previousKey[index].getClass(), ObjectInspectorOptions.JAVA)) != 0) {

					return false;
				}

			}
			status = true;
		}
		return status;
	}

	/**
	 * This will help us compare the currentKey and previousKey objects.
	 *
	 * @param currentKey
	 * @return - true if both are same else false
	 * @throws HiveException
	 */
	private boolean sameValue(DeferredObject[] currentKey) throws HiveException {
		boolean status = false;

		//if both are null then we can classify as same
		if (currentKey == null && previousKey == null) {
			status = true;
		}

		//if both are not null and there legnth as well as
		//individual elements are same then we can classify as same
		if (currentKey != null && previousKey != null && currentKey.length == previousKey.length) {
			//				for (int index = 1; index < currentKey.length; index++) {

			if (ObjectInspectorUtils.compare(currentKey[0].get(), this.ois[0],
					previousKey[0],
					ObjectInspectorFactory.getReflectionObjectInspector(previousKey[0].getClass(), ObjectInspectorOptions.JAVA)) != 0) {

				return false;
			}

			//				}
			status = true;
		}
		return status;
	}

}
