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

package com.google.zetasql.toolkit.catalog.io;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.zetasql.FunctionArgumentType;
import com.google.zetasql.FunctionArgumentType.FunctionArgumentTypeOptions;
import com.google.zetasql.FunctionSignature;
import com.google.zetasql.SimpleColumn;
import com.google.zetasql.SimpleTable;
import com.google.zetasql.TVFRelation;
import com.google.zetasql.Type;
import com.google.zetasql.TypeFactory;
import com.google.zetasql.ZetaSQLFunctions.FunctionEnums.Mode;
import com.google.zetasql.ZetaSQLFunctions.FunctionEnums.NamedArgumentKind;
import com.google.zetasql.ZetaSQLFunctions.SignatureArgumentKind;
import com.google.zetasql.ZetaSQLType.TypeKind;
import com.google.zetasql.toolkit.catalog.FunctionInfo;
import com.google.zetasql.toolkit.catalog.ProcedureInfo;
import com.google.zetasql.toolkit.catalog.TVFInfo;
import com.google.zetasql.toolkit.catalog.typeparser.ZetaSQLTypeParseError;
import com.google.zetasql.toolkit.catalog.typeparser.ZetaSQLTypeParser;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Implements deserialization of {@link CatalogResources} from JSON objects. JSON objects
 * representing catalogs support tables, scalar functions, TVFs and procedures.
 *
 * <pre>
 * JSON objects should follow the following format:
 *
 * {
 *   "tables": [
 *     {
 *       "name": "project.dataset.table",
 *       "columns": [
 *         { "name": "column", "type": "INT64" },
 *         { "name": "column2", "type": "TIMESTAMP", "isPseudoColumn": true }
 *       ]
 *     }
 *   ],
 *   "functions": [
 *     {
 *       "name": "project.dataset.function",
 *       "signatures": [
 *         {
 *           "arguments": [
 *             {  "name": "arg1", "type": "ARRAY&lt;STRING&gt;" }
 *           ],
 *           "returnType": "STRING"
 *         }
 *       ]
 *     }
 *   ],
 *   "tvfs": [
 *     {
 *       "name": "project.dataset.tvf",
 *       "arguments": [
 *         { "name": "arg1", "type": "ARRAY&lt;STRUCT&lt;x INT64&gt;&gt;" }
 *       ],
 *       "outputColumns": [
 *         { "name": "out1", "type": "INT64" }
 *       ]
 *     }
 *   ],
 *   "procedures": [
 *     {
 *       "name": "project.dataset.proc",
 *       "arguments": [
 *         { "name": "arg1", "type": "ARRAY&lt;STRUCT&lt;x INT64&gt;&gt;" }
 *       ]
 *     }
 *   ]
 * }
 * </pre>
 *
 */
class JsonCatalogDeserializer {

  /**
   * {@link Gson} instance used when deserializing. Registers all custom deserializers defined in
   * this class.
   */
  private static final Gson gson = new GsonBuilder()
      .registerTypeAdapter(SimpleTable.class, new TableDeserializer())
      .registerTypeAdapter(FunctionInfo.class, new FunctionDeserializer())
      .registerTypeAdapter(FunctionSignature.class, new FunctionSignatureDeserializer())
      .registerTypeAdapter(FunctionArgumentType.class, new FunctionArgumentTypeDeserializer())
      .registerTypeAdapter(TVFInfo.class, new TVFDeserializer())
      .registerTypeAdapter(ProcedureInfo.class, new ProcedureDeserializer())
      .create();

  /**
   * Deserializes the {@link CatalogResources} represented by the provided JSON object
   *
   * @param json The JSON string for the object to deserialize into {@link CatalogResources}
   * @return The resulting {@link CatalogResources} instance
   * @throws JsonParseException when parsing the JSON catalog fails
   */
  public static CatalogResources readJsonCatalog(String json) throws JsonParseException {
    return gson.fromJson(json, CatalogResources.class);
  }

  private static JsonObject getAsJsonObject(JsonElement jsonElement, String errorMessage) {
    if (!jsonElement.isJsonObject()) {
      throw new JsonParseException(errorMessage);
    }

    return jsonElement.getAsJsonObject();
  }

  private static JsonArray getFieldAsJsonArray(
      JsonObject jsonObject, String fieldName, String errorMessage) {
    return Optional.ofNullable(jsonObject.get(fieldName))
        .filter(JsonElement::isJsonArray)
        .map(JsonElement::getAsJsonArray)
        .orElseThrow(() -> new JsonParseException(errorMessage));
  }

  private static String getFieldAsString(
      JsonObject jsonObject, String fieldName, String errorMessage) {
    JsonElement field = jsonObject.get(fieldName);

    if (Objects.isNull(field)
        || !field.isJsonPrimitive()
        || !field.getAsJsonPrimitive().isString()) {
      throw new JsonParseException(errorMessage);
    }

    return field.getAsString();
  }

  private static boolean getFieldAsBoolean(
      JsonObject jsonObject, String fieldName, String errorMessage) {
    JsonElement field = jsonObject.get(fieldName);

    if (Objects.isNull(field) || !field.isJsonPrimitive()) {
      throw new JsonParseException(errorMessage);
    }

    JsonPrimitive primitive = field.getAsJsonPrimitive();
    boolean isTrueOrFalseString = primitive.isString()
        && List.of("true", "false").contains(primitive.getAsString().toLowerCase());

    if (!primitive.isBoolean() && !isTrueOrFalseString) {
      throw new JsonParseException(errorMessage);
    }

    return primitive.getAsBoolean();
  }

  private static Type parseType(String type) {
    try {
      return ZetaSQLTypeParser.parse(type);
    } catch (ZetaSQLTypeParseError err) {
      throw new JsonParseException("Invalid SQL type: " + type, err);
    }
  }

  private static SimpleColumn deserializeSimpleColumn(String tableName, JsonObject jsonColumn) {
    String columnName = getFieldAsString(
        jsonColumn, "name",
        "Invalid JSON column " + jsonColumn + ". Field name should be string");
    String columnType = getFieldAsString(
        jsonColumn, "type",
        "Invalid JSON column " + jsonColumn + ". Field type should be string");

    Type parsedType = parseType(columnType);

    boolean isPseudoColumn = jsonColumn.has("isPseudoColumn")
        && getFieldAsBoolean(jsonColumn, "isPseudoColumn",
          "Invalid JSON column " + jsonColumn + ". Field isPseudoColumn should be bool");

    boolean isWriteableColumn = !isPseudoColumn;

    return new SimpleColumn(tableName, columnName, parsedType, isPseudoColumn, isWriteableColumn);
  }

  private static TVFRelation.Column deserializeTVFOutputColumn(JsonObject jsonColumn) {
    SimpleColumn deserializedSimpleColumn = deserializeSimpleColumn("", jsonColumn);
    return TVFRelation.Column.create(
        deserializedSimpleColumn.getName(),
        deserializedSimpleColumn.getType());
  }

  /**
   * Gson deserializer for {@link SimpleTable}
   */
  private static class TableDeserializer implements JsonDeserializer<SimpleTable> {

    @Override
    public SimpleTable deserialize(
        JsonElement jsonElement,
        java.lang.reflect.Type type,
        JsonDeserializationContext context
    ) throws JsonParseException {
      JsonObject jsonObject = getAsJsonObject(
          jsonElement,
          "Invalid JSON table: " + jsonElement + ". Tables should be objects.");

      String tableName = getFieldAsString(
          jsonObject, "name",
          "Invalid JSON table: " + jsonElement + ". Field name should be string.");

      JsonArray columns = getFieldAsJsonArray(
          jsonObject, "columns",
          "Invalid JSON table: " + jsonElement + ". Field columns should be array of columns.");

      List<SimpleColumn> parsedColumns = columns
          .asList()
          .stream()
          .map(jsonColumn ->
              getAsJsonObject(jsonColumn,
                  "Invalid JSON column " + jsonColumn + ". Should be JSON object."))
          .map(jsonColumn -> deserializeSimpleColumn(tableName, jsonColumn))
          .collect(Collectors.toList());

      return new SimpleTable(tableName, parsedColumns);
    }
  }

  /**
   * Gson deserializer for {@link FunctionInfo}
   */
  private static class FunctionDeserializer implements JsonDeserializer<FunctionInfo> {

    @Override
    public FunctionInfo deserialize(
        JsonElement jsonElement,
        java.lang.reflect.Type type,
        JsonDeserializationContext context
    ) throws JsonParseException {
      JsonObject jsonObject = getAsJsonObject(
          jsonElement,
          "Invalid JSON function: " + jsonElement + ". Functions should be objects.");

      String functionName = getFieldAsString(
          jsonObject, "name",
          "Invalid JSON function: " + jsonElement + ". Field name should be string.");

      FunctionSignature[] signatures = Optional.ofNullable(jsonObject.get("signatures"))
          .map(jsonSignatures -> context.deserialize(jsonSignatures, FunctionSignature[].class))
          .map(FunctionSignature[].class::cast)
          .orElseThrow(() -> new JsonParseException(
              "Invalid JSON function: " + jsonElement + ". Signatures missing."));

      return FunctionInfo.newBuilder()
          .setNamePath(List.of(functionName))
          .setGroup("UDF")
          .setMode(Mode.SCALAR)
          .setSignatures(Arrays.asList(signatures))
          .build();
    }
  }

  /**
   * Gson deserializer for {@link FunctionSignature}
   */
  private static class FunctionSignatureDeserializer implements JsonDeserializer<FunctionSignature> {

    @Override
    public FunctionSignature deserialize(
        JsonElement jsonElement,
        java.lang.reflect.Type type,
        JsonDeserializationContext context
    ) throws JsonParseException {
      JsonObject jsonObject = getAsJsonObject(
          jsonElement,
          "Invalid JSON function signature: " + jsonElement + ". Should be object.");

      String returnType = getFieldAsString(
          jsonObject, "returnType",
          "Invalid JSON function signature: " + jsonElement
              + ". Field returnType should be string.");

      Type parsedReturnType = parseType(returnType);

      FunctionArgumentType[] arguments = Optional.ofNullable(jsonObject.get("arguments"))
          .map(jsonArguments -> context.deserialize(jsonArguments, FunctionArgumentType[].class))
          .map(FunctionArgumentType[].class::cast)
          .orElseThrow(() -> new JsonParseException(
              "Invalid JSON function signature: " + jsonElement + ". Arguments missing."));

      return new FunctionSignature(
          new FunctionArgumentType(parsedReturnType),
          Arrays.asList(arguments),
          -1);

    }
  }

  /**
   * Gson deserializer for {@link FunctionArgumentType}
   */
  private static class FunctionArgumentTypeDeserializer
      implements JsonDeserializer<FunctionArgumentType> {

    @Override
    public FunctionArgumentType deserialize(
        JsonElement jsonElement,
        java.lang.reflect.Type type,
        JsonDeserializationContext context
    ) throws JsonParseException {
      JsonObject jsonObject = getAsJsonObject(
          jsonElement,
          "Invalid JSON function argument: " + jsonElement + ". Should be object.");

      String argumentName = getFieldAsString(
          jsonObject, "name",
          "Invalid JSON function argument: " + jsonElement + ". Field name should be string.");

      String argumentType = getFieldAsString(
          jsonObject, "type",
          "Invalid JSON function argument: " + jsonElement + ". Field type should be string.");

      Type parsedArgumentType = parseType(argumentType);

      return new FunctionArgumentType(
          parsedArgumentType,
          FunctionArgumentTypeOptions.builder()
              .setArgumentName(argumentName, NamedArgumentKind.POSITIONAL_OR_NAMED)
              .build(),
          1
      );
    }
  }

  /**
   * Gson deserializer for {@link TVFInfo}
   */
  private static class TVFDeserializer implements JsonDeserializer<TVFInfo> {

    @Override
    public TVFInfo deserialize(
        JsonElement jsonElement,
        java.lang.reflect.Type type,
        JsonDeserializationContext context
    ) throws JsonParseException {
      JsonObject jsonObject = getAsJsonObject(
          jsonElement,
          "Invalid JSON TVF: " + jsonElement + ". TVFs should be objects.");

      String functionName = getFieldAsString(
          jsonObject, "name",
          "Invalid JSON TVF: " + jsonElement + ". Field name should be string.");

      FunctionArgumentType[] arguments = Optional.ofNullable(jsonObject.get("arguments"))
          .map(jsonArguments -> context.deserialize(jsonArguments, FunctionArgumentType[].class))
          .map(FunctionArgumentType[].class::cast)
          .orElseThrow(() -> new JsonParseException(
              "Invalid JSON TVF: " + jsonElement + ". Arguments missing."));

      JsonArray outputColumns = getFieldAsJsonArray(
          jsonObject, "outputColumns",
          "Invalid JSON TVF: " + jsonElement + ". Field outputColumns should be array of columns.");

      List<TVFRelation.Column> parsedOutputColumns = outputColumns
          .asList()
          .stream()
          .map(jsonColumn ->
              getAsJsonObject(jsonColumn,
                  "Invalid JSON column " + jsonColumn + ". Should be JSON object."))
          .map(JsonCatalogDeserializer::deserializeTVFOutputColumn)
          .collect(Collectors.toList());

      FunctionArgumentType returnType =
          new FunctionArgumentType(
              SignatureArgumentKind.ARG_TYPE_RELATION,
              FunctionArgumentTypeOptions.builder().build(),
              1);

      return TVFInfo.newBuilder()
          .setNamePath(ImmutableList.of(functionName))
          .setSignature(new FunctionSignature(returnType, Arrays.asList(arguments), -1))
          .setOutputSchema(TVFRelation.createColumnBased(parsedOutputColumns))
          .build();
    }
  }

  /**
   * Gson deserializer for {@link ProcedureInfo}
   */
  private static class ProcedureDeserializer implements JsonDeserializer<ProcedureInfo> {

    @Override
    public ProcedureInfo deserialize(
        JsonElement jsonElement,
        java.lang.reflect.Type type,
        JsonDeserializationContext context
    ) throws JsonParseException {
      JsonObject jsonObject = getAsJsonObject(
          jsonElement,
          "Invalid JSON procedure: " + jsonElement + ". Preceduress should be objects.");

      String procedureName = getFieldAsString(
          jsonObject, "name",
          "Invalid JSON procedure: " + jsonElement + ". Field name should be string.");

      FunctionArgumentType[] arguments = Optional.ofNullable(jsonObject.get("arguments"))
          .map(jsonArguments -> context.deserialize(jsonArguments, FunctionArgumentType[].class))
          .map(FunctionArgumentType[].class::cast)
          .orElseThrow(() -> new JsonParseException(
              "Invalid JSON procedure: " + jsonElement + ". Arguments missing."));

      FunctionArgumentType returnType =
          new FunctionArgumentType(TypeFactory.createSimpleType(TypeKind.TYPE_STRING));

      FunctionSignature signature = new FunctionSignature(
          returnType, Arrays.asList(arguments), -1);

      return new ProcedureInfo(ImmutableList.of(procedureName), signature);
    }
  }

}
