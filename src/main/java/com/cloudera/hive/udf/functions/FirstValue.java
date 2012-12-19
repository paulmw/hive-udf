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

package com.cloudera.hive.udf.functions;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

/**
 * This UDF provides a first_value() function.
 */
@Description(name = "first_value", value = "_FUNC_(value, optional partition columns ...) - Returns the first_value of a column within a partitioned, sorted window.")
@UDFType(deterministic = false, stateful = true)
public class FirstValue extends GenericUDF {

	private boolean firstRow;
	private Object value;
	private Object[] previous;
	private ObjectInspector[] ois;

	@Override
	public ObjectInspector initialize(ObjectInspector[] ois) throws UDFArgumentException {
		this.firstRow = true;
		this.ois = ois;
		GenericUDFUtils.ReturnObjectInspectorResolver roir = new GenericUDFUtils.ReturnObjectInspectorResolver(true);
		roir.update(ois[0]);
		return roir.get();
	}

	/**
	 * This expects multiple parameters: the first should be the value cache, the rest should be the PARTITION BY columns.
	 */
	@Override
	public Object evaluate(DeferredObject[] current) throws HiveException {
		if(firstRow) {
			this.value = ObjectInspectorUtils.copyToStandardObject(current[0].get(), this.ois[0]);
			firstRow = false;
			copyToPreviousKey(current);
		} else {
			if (!groupIsUnchanged(current)) {
				this.value = ObjectInspectorUtils.copyToStandardObject(current[0].get(), this.ois[0]);
			}
			copyToPreviousKey(current);
		}
		return value;
	}

	@Override
	public String getDisplayString(String[] currentKey) {
		return "FV";
	}



	/**
	 * This will help us copy objects from currrentKey to previousKeyHolder.
	 *
	 * @param currentKey
	 * @throws HiveException
	 */
	private void copyToPreviousKey(DeferredObject[] currentKey) throws HiveException {
		if (currentKey != null) {
			previous = new Object[currentKey.length];
			for (int index = 0; index < currentKey.length; index++) {   
				previous[index] = ObjectInspectorUtils.copyToStandardObject(currentKey[index].get(), this.ois[index]);
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
	private boolean groupIsUnchanged(DeferredObject[] currentKey) throws HiveException {
		boolean status = false;

		//if both are null then we can classify as same
		if (currentKey == null && previous == null) {
			status = true;
		}

		//if both are not null and their lengths as well as
		//individual elements are same then we can classify as same
		if (currentKey != null && previous != null && currentKey.length == previous.length) {
			for (int index = 1; index < currentKey.length; index++) { // Note the 1 here! INDEX 0 is the value, INDEX 1+ are the partition columns

				if (ObjectInspectorUtils.compare(currentKey[index].get(), this.ois[index],
						previous[index],
						ObjectInspectorFactory.getReflectionObjectInspector(previous[index].getClass(), ObjectInspectorOptions.JAVA)) != 0) {

					return false;
				}

			}
			status = true;
		}
		return status;
	}
}
