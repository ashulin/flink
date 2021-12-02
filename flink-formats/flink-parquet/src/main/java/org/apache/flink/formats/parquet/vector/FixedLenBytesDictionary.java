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

package org.apache.flink.formats.parquet.vector;

import org.apache.flink.formats.parquet.vector.reader.FixedLenBytesColumnReader;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.vector.Dictionary;

/** This dictionary for {@link FixedLenBytesColumnReader}. */
public class FixedLenBytesDictionary implements Dictionary {

    private org.apache.parquet.column.Dictionary dictionary;

    public FixedLenBytesDictionary(org.apache.parquet.column.Dictionary dictionary) {
        this.dictionary = dictionary;
    }

    @Override
    public int decodeToInt(int id) {
        return dictionary.decodeToBinary(id).toByteBuffer().getInt();
    }

    @Override
    public long decodeToLong(int id) {
        return dictionary.decodeToBinary(id).toByteBuffer().getLong();
    }

    @Override
    public byte[] decodeToBinary(int id) {
        return dictionary.decodeToBinary(id).getBytes();
    }

    @Override
    public float decodeToFloat(int id) {
        throw new UnsupportedOperationException(this.getClass().getName());
    }

    @Override
    public double decodeToDouble(int id) {
        throw new UnsupportedOperationException(this.getClass().getName());
    }

    @Override
    public TimestampData decodeToTimestamp(int id) {
        throw new UnsupportedOperationException(this.getClass().getName());
    }
}
