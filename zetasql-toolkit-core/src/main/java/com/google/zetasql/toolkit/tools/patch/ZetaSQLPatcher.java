/*
 * Copyright 2023 Google LLC All Rights Reserved
 *
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

package com.google.zetasql.toolkit.tools.patch;

import com.google.protobuf.ExperimentalApi;
import com.google.protobuf.Message;
import com.google.zetasql.LocalService.AnalyzeRequest;
import com.google.zetasql.LocalService.AnalyzeResponse;
import com.google.zetasql.LocalService.BuildSqlRequest;
import com.google.zetasql.LocalService.BuildSqlResponse;
import com.google.zetasql.LocalService.ParseRequest;
import com.google.zetasql.LocalService.ParseResponse;
import com.google.zetasql.ZetaSqlLocalServiceGrpc;
import com.google.zetasql.io.grpc.MethodDescriptor;
import com.google.zetasql.io.grpc.MethodDescriptor.Marshaller;
import com.google.zetasql.io.grpc.protobuf.ProtoUtils;
import java.lang.reflect.Field;

/**
 * Implements some reflection-based patches to ZetaSQL. The only patch currently available is {@link
 * #patchMaxProtobufNestingDepth(int)}.
 *
 * <p>These reflection-based patches are brittle by design and should be avoided whenever possible.
 */
public class ZetaSQLPatcher {

  /**
   * Patches the maximum protobuf nesting depth allowed when accessing the ZetaSQL local service
   * through GRPC. This patch should be applied when working with large SQL statements that hit
   * GRPC's default nesting limit of 100.
   *
   * <p>This new max depth is only set for three key RPCs, which result in a lot of nesting when
   * working with large SQL statements. These RPCs are Parse, Analyze and BuildSql.
   *
   * <p>This patch should be considered experimental; as it relies on {@link
   * ProtoUtils#marshallerWithRecursionLimit(Message, int)} from grpc-java, which is experimental
   * itself.
   *
   * @param maxDepth the maximum nesting depth to set for RPCs to the ZetaSQL local service. Should
   *     be greater than 100.
   * @throws IllegalAccessException if any reflective access performed by this patch produces an
   *     illegal access.
   */
  @ExperimentalApi
  public static void patchMaxProtobufNestingDepth(int maxDepth) throws IllegalAccessException {
    if (!(maxDepth > 100)) {
      throw new IllegalArgumentException(
          "Invalid max nesting depth for patching protobuf. Should be at least 100, but got: "
              + maxDepth);
    }

    patchMaxNestingDepthForParse(maxDepth);
    patchMaxNestingDepthForAnalyze(maxDepth);
    patchMaxNestingDepthForBuildSql(maxDepth);
  }

  private static Field getLocalServiceField(String name) {
    try {
      return ZetaSqlLocalServiceGrpc.class.getDeclaredField(name);
    } catch (NoSuchFieldException noSuchFieldException) {
      throw new IllegalStateException(
          "Tried to access not existent field in class ZetaSqlLocalServiceGrpc: " + name,
          noSuchFieldException);
    }
  }

  private static void patchMaxNestingDepthForAnalyze(int maxDepth) throws IllegalAccessException {
    Field getAnalyzeMethod = getLocalServiceField("getAnalyzeMethod");

    Marshaller<AnalyzeRequest> requestMarshaller =
        ProtoUtils.marshallerWithRecursionLimit(AnalyzeRequest.getDefaultInstance(), maxDepth);
    Marshaller<AnalyzeResponse> responseMarshaller =
        ProtoUtils.marshallerWithRecursionLimit(AnalyzeResponse.getDefaultInstance(), maxDepth);

    MethodDescriptor<AnalyzeRequest, AnalyzeResponse> newMethodDescriptor =
        ZetaSqlLocalServiceGrpc.getAnalyzeMethod()
            .toBuilder(requestMarshaller, responseMarshaller)
            .build();

    synchronized (ZetaSqlLocalServiceGrpc.class) {
      getAnalyzeMethod.setAccessible(true);
      getAnalyzeMethod.set(null, newMethodDescriptor);
    }
  }

  private static void patchMaxNestingDepthForParse(int maxDepth) throws IllegalAccessException {
    Field getParseMethod = getLocalServiceField("getParseMethod");

    Marshaller<ParseRequest> requestMarshaller =
        ProtoUtils.marshallerWithRecursionLimit(ParseRequest.getDefaultInstance(), maxDepth);
    Marshaller<ParseResponse> responseMarshaller =
        ProtoUtils.marshallerWithRecursionLimit(ParseResponse.getDefaultInstance(), maxDepth);

    MethodDescriptor<ParseRequest, ParseResponse> newMethodDescriptor =
        ZetaSqlLocalServiceGrpc.getParseMethod()
            .toBuilder(requestMarshaller, responseMarshaller)
            .build();

    synchronized (ZetaSqlLocalServiceGrpc.class) {
      getParseMethod.setAccessible(true);
      getParseMethod.set(null, newMethodDescriptor);
    }
  }

  private static void patchMaxNestingDepthForBuildSql(int maxDepth) throws IllegalAccessException {
    Field getBuildSqlMethod = getLocalServiceField("getBuildSqlMethod");

    Marshaller<BuildSqlRequest> requestMarshaller =
        ProtoUtils.marshallerWithRecursionLimit(BuildSqlRequest.getDefaultInstance(), maxDepth);
    Marshaller<BuildSqlResponse> responseMarshaller =
        ProtoUtils.marshallerWithRecursionLimit(BuildSqlResponse.getDefaultInstance(), maxDepth);

    MethodDescriptor<BuildSqlRequest, BuildSqlResponse> newMethodDescriptor =
        ZetaSqlLocalServiceGrpc.getBuildSqlMethod()
            .toBuilder(requestMarshaller, responseMarshaller)
            .build();

    synchronized (ZetaSqlLocalServiceGrpc.class) {
      getBuildSqlMethod.setAccessible(true);
      getBuildSqlMethod.set(null, newMethodDescriptor);
    }
  }
}
