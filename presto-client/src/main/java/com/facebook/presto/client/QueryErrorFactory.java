/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.client;

import com.facebook.presto.spi.PrestoException;

import java.util.Arrays;
import java.util.stream.Collectors;

public class QueryErrorFactory
{
    private QueryErrorFactory() {}

    public static QueryError createQueryError(PrestoException e)
    {
        return new QueryError(
                e.getMessage(),
                null,
                e.getErrorCode().getCode(),
                e.getErrorCode().getName(),
                e.getErrorCode().getType().toString(),
                null,
                failureInfoFromThrowable(e));
    }

    private static FailureInfo failureInfoFromThrowable(Throwable e)
    {
        if (e == null) {
            return null;
        }

        if (e instanceof PrestoException) {
            return failureInfoFromPrestoException((PrestoException) e);
        }

        if (e instanceof Exception) {
            return failureInfoFromAnyException((RuntimeException) e);
        }

        return null;
    }

    private static FailureInfo failureInfoFromPrestoException(PrestoException e)
    {
        return new FailureInfo(
                e.getErrorCode().getType().toString(),
                e.getMessage(),
                failureInfoFromThrowable(e.getCause()),
                Arrays.stream(e.getSuppressed()).map(x -> failureInfoFromThrowable(x)).collect(Collectors.toList()),
                Arrays.stream(e.getStackTrace()).map(x -> x.toString()).collect(Collectors.toList()),
                null);
    }

    private static FailureInfo failureInfoFromAnyException(Exception e)
    {
        return new FailureInfo(
                "UNKNOWN",
                e.getMessage(),
                failureInfoFromThrowable(e.getCause()),
                Arrays.stream(e.getSuppressed()).map(x -> failureInfoFromThrowable(x)).collect(Collectors.toList()),
                Arrays.stream(e.getStackTrace()).map(x -> x.toString()).collect(Collectors.toList()),
                null);
    }
}
