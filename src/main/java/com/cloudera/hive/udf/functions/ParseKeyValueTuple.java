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

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

import java.util.*;

/**
 * This UDTF provides a function to parse a string of delimited key value pairs.</br>
 * An example of this would be a URL Query string or a string representation of a cookie.</br>
 * <P>
 * Note: Both static and dynamic input parameters are supported.
 * <P>
 * Example Strings:</br>
 *  URL Query: "KEY1=val1&KEY2=val2&KEY3=val3"</br>
 *  Cookie String: "KEY1=val1;KEY2=val2;KEY3=val3"</br>
 * <P>
 * Example Queries:</br>
 *  URL Query: "SELECT b.* FROM src LATERAL VIEW _FUNC_(inputString, '&', '=', 'KEY1', 'KEY2', 'KEY3') b as key1, key2, key3 LIMIT 1;"</br>
 *  Cookie String: "SELECT b.* FROM src LATERAL VIEW _FUNC_(inputString, '\;', '=', 'KEY1', 'KEY2', 'KEY3') b as key1, key2, key3 LIMIT 1;"
 *
 *  @see org.apache.hadoop.hive.ql.udf.generic.GenericUDTF
 */
@Description(name = "parse_key_val_tuple",
        value = "_FUNC_(inputString, fieldDelimiter, keyValSeparator, keyName, keyName2, ..., keyNameN) - extracts N (N>=1) parts from a delimited key value String.\n"
                + "It takes an inputString, fieldDelimiter, keyValSeparator, and one or multiple keyNames, and returns a tuple. "
                + "All the input parameters and output column types are string.",
        extended = "Note: All parameters are case-sensitive, and should not contain unnecessary white spaces.\n"
                + "Note: Delimiter and separator characters such as ';' may need to be escaped\n"
                + "Example:\n"
                + "  > SELECT b.* FROM src LATERAL VIEW _FUNC_(inputString, '\\;', '=', 'KEY1', 'KEY2', 'KEY3') b as key1, key2, key3 LIMIT 1;")
public class ParseKeyValueTuple extends GenericUDTF {
    private static final Log LOG = LogFactory.getLog(ParseKeyValueTuple.class.getName());
    private static final String FUNCTION_NAME = "parse_key_val_tuple";
    private static final int STATIC_ARG_COUNT = 3;
    private static final String REQUIRED_TYPE = "string";

    private transient ObjectInspector[] inputOIs; // Input ObjectInspectors
    private int numCols;    // Number of output columns
    private Text[] cols;    // Object pool of non-null Text, avoid creating objects all the time
    private transient Object[] nullCols; // Array of null column values (returned during errors)
    private boolean nullWarned = false;
    private boolean mapWarned = false;

    @Override
    public void close() {
    }

    /**
     * Initializes the UDTF fields and builds the StructObjectInspector for the output columns.
     *
     * @param args the UDTF args
     * @return output column StructObjectInspector
     * @throws UDFArgumentException when the arguments are invalid
     */
    @Override
    public StructObjectInspector initialize(final ObjectInspector[] args) throws UDFArgumentException {
        validateArgs(args);
        // Initialize fields
        inputOIs = args;
        numCols = args.length - STATIC_ARG_COUNT;
        cols = new Text[numCols];
        nullCols = new Object[numCols];
        nullWarned = false;
        mapWarned = false;
        // Fill arrays
        for (int i = 0; i < numCols; ++i) {
            cols[i] = new Text();
            nullCols[i] = null;
        }
        return createOutputObjectInspector();
    }

    /**
     * Validates the arity and type of the input arguments.
     *
     * @param args the arguments to validate
     * @throws UDFArgumentException when the arguments are invalid
     */
    private void validateArgs(final ObjectInspector[] args) throws UDFArgumentException {
        //Validate argument arity
        if (args.length < STATIC_ARG_COUNT + 1) {
            throw new UDFArgumentException(FUNCTION_NAME + " takes at least" + STATIC_ARG_COUNT + 1 + "arguments: the string, fieldDelimiter, pairDelimiter, and a key name");
        }
        // Validate all arguments are string type
        for(final ObjectInspector arg: args) {
            if (arg.getCategory() != ObjectInspector.Category.PRIMITIVE || !REQUIRED_TYPE.equals(arg.getTypeName())) {
                throw new UDFArgumentException(FUNCTION_NAME +"'s arguments have to be " + REQUIRED_TYPE + " type");
            }
        }
    }

    /**
     * Creates an output StructObjectInspector based on the number of columns needed.
     * Assumes all output types will be Text.
     *
     * @return an output StructObjectInspector
     */
    private StructObjectInspector createOutputObjectInspector() {
        final ArrayList<String> fieldNames = new ArrayList<String>(numCols);
        final ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(numCols);
        for (int i = 0; i < numCols; ++i) {
            fieldNames.add("c" + i); // column name can be anything since it will be named by the UDTF "as" clause
            fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector); // all returned type will be Text
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    /**
     * Process the UDTF input values and forward the resulting rows.
     *
     * @param o UDTF input values
     * @throws HiveException
     */
    @Override
    public void process(final Object[] o) throws HiveException {
        if (o[0] == null) {
            forward(nullCols);
            return;
        }

        // Get UDTF input values
        final String inputStr = getStringFromInputObjects(o, 0);
        final String fieldDelimiter = getStringFromInputObjects(o, 1);
        final String keyValSeparator = getStringFromInputObjects(o, 2);
        final List<String> keyNames = getKeyNamesFromInputObjects(o);

        if (inputValueIsEmpty(inputStr, fieldDelimiter, keyValSeparator, keyNames)) {
            if(!nullWarned) {
                LOG.warn("At least 1 Null row returned. An input argument was empty. Additional warnings for a null row will be suppressed.");
                nullWarned = true;
            }
            forward(nullCols);
            return;
        }

        final Map<String, String> keyValMap = getKeyValMap(inputStr, fieldDelimiter, keyValSeparator, keyNames);
        final Text[] returnColumns = getReturnColumnValues(keyNames, keyValMap);
        forward(returnColumns);
    }

    /**
     * Gets an array of the keyNames from the passed object array based on UDTF arguments.
     *
     * @param o input objects
     * @return a list of keyNames
     */
    private List<String> getKeyNamesFromInputObjects(final Object[] o) {
        final ImmutableList.Builder<String> builder = new ImmutableList.Builder<String>();
        for (int i = 0; i < numCols; i++) {
            final String keyName = getStringFromInputObjects(o, i + STATIC_ARG_COUNT);
            builder.add(keyName);
        }
        return builder.build();
    }

    /**
     * Gets a string value from the passed object array based on UDTF arguments and an index.
     *
     * @param o input objects
     * @param i index to retrieve
     * @return the string value
     */
    private String getStringFromInputObjects(final Object[] o, final int i) {
        return ((StringObjectInspector) inputOIs[i]).getPrimitiveJavaObject(o[i]);
    }

    /**
     * Returns true if any of the input strings are empty.
     *
     * @param inputStr        the input string
     * @param fieldDelimiter  the field delimiter
     * @param keyValSeparator the key value separator
     * @param keyNames        the key names
     * @return true if any of the string are empty
     */
    private boolean inputValueIsEmpty(final String inputStr, final String fieldDelimiter, final String keyValSeparator, final List<String> keyNames) {
        return StringUtils.isEmpty(inputStr) ||
                StringUtils.isEmpty(fieldDelimiter) ||
                StringUtils.isEmpty(keyValSeparator) ||
                keyNames.contains("");
    }

    /**
     * Processes the input string into a KeyValue Map utilizing the fieldDelimiter and keyValSeparator.</br>
     * Only considers valid pairs(has keyValSeparator) with non-empty/null keys that are in keyNames.
     * <p>
     * Note: The key is the string before the first occurrence of keyValSeparator and the value is everything after.</br>
     * Note: If a key occurs twice the last value seen will be represented.
     *
     * @param inputString     the string to be processed
     * @param fieldDelimiter  separator between KeyValue pairs
     * @param keyValSeparator separator between key and value
     * @param keyNames        used to filter values inserted
     * @return the key value map for keyNames
     */
    private Map<String, String> getKeyValMap(final String inputString, final String fieldDelimiter, final String keyValSeparator, final List<String> keyNames) {
        final Set<String> uniqueKeyNames = new HashSet<String>(keyNames); // Optimize in the case of duplicate key names
        final Map<String, String> keyValMap = new HashMap<String, String>(uniqueKeyNames.size()); //Initialized with the expected size
        final Iterable<String> splitIterable = Splitter.on(fieldDelimiter).omitEmptyStrings().split(inputString); //Iterator to prevent excessive allocation
        int count = 0; // Counter to break out when we have seen all of the uniqueKeyNames
        for (final String keyValPair : splitIterable) {
            final String key = StringUtils.substringBefore(keyValPair, keyValSeparator);
            final String value = StringUtils.substringAfter(keyValPair, keyValSeparator);
            // Only consider valid pairs with non-empty/null keys that are in uniqueKeyNames
            if (StringUtils.contains(keyValPair, keyValSeparator) && !StringUtils.isEmpty(key) && uniqueKeyNames.contains(key) ) {
                final String prev = keyValMap.put(key, value);
                if(prev == null) {
                    count++;
                } else if(!mapWarned) { // Otherwise a key was replaced
                    LOG.warn("At least 1 inputString had a duplicate key for a keyName. The second value will be represented. Additional warnings for a duplicate key will be suppressed.");
                    mapWarned = true;
                }
                if (count >= uniqueKeyNames.size()) {
                    break; // We have seen all of the keyNames needed
                }
            }
        }
        return keyValMap;
    }

    /**
     * Retrieves all of the column values to be returned.
     *
     * @param keyNames  keyNames of the return columns
     * @param keyValMap KeyValMap containing the values
     * @return Text array of return column values
     */
    private Text[] getReturnColumnValues(final List<String> keyNames, final Map<String, String> keyValMap) {
        final Text[] returnColumns = new Text[numCols];
        for (int i = 0; i < numCols; ++i) {
            final String ret = keyValMap.get(keyNames.get(i));
            if (ret == null) {
                returnColumns[i] = null;
            } else {
                if (returnColumns[i] == null) {
                    returnColumns[i] = cols[i]; // Use the object pool rather than creating a new object
                }
                returnColumns[i].set(ret);
            }
        }
        return returnColumns;
    }

    /**
     * Returns the name of the UDTF function.
     *
     * @return name of the UDTF function
     */
    @Override
    public String toString() {
        return FUNCTION_NAME;
    }
}