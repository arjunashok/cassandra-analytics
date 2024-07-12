/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cassandra.spark.utils;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.StandardCharsets;

import io.netty.util.concurrent.FastThreadLocal;

public final class FastThreadLocalUtf8Decoder
{
    private FastThreadLocalUtf8Decoder()
    {
        super();
        throw new IllegalStateException(getClass() + " is static utility class and shall not be instantiated");
    }

    private static final FastThreadLocal<CharsetDecoder> UTF8_DECODER = new FastThreadLocal<CharsetDecoder>()
    {
        @Override
        protected CharsetDecoder initialValue()
        {
            return StandardCharsets.UTF_8.newDecoder();
        }
    };

    public static String string(ByteBuffer buffer) throws CharacterCodingException
    {
        return ByteBufferUtils.string(buffer, FastThreadLocalUtf8Decoder.UTF8_DECODER::get);
    }

    public static String stringThrowRuntime(ByteBuffer buffer)
    {
        try
        {
            return string(buffer);
        }
        catch (CharacterCodingException exception)
        {
            throw new RuntimeException(exception);
        }
    }
}
