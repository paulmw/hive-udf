package com.cloudera.hive.examples;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.ObjectInspectorOptions;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

public class FirstValue extends GenericUDF {

	private boolean firstRow;
	private Object value;
	private Object[] previous;
	private ObjectInspector[] ois;

	@Override
	public ObjectInspector initialize(ObjectInspector[] ois) throws UDFArgumentException {
		this.firstRow = true;
		this.ois = ois;
		return ois[0];
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
			for (int index = 1; index < currentKey.length; index++) { // Note the 1 here!

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
