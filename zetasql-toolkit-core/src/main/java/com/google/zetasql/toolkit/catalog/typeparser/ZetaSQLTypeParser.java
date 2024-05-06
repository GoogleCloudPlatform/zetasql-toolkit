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

package com.google.zetasql.toolkit.catalog.typeparser;

import com.google.zetasql.StructType.StructField;
import com.google.zetasql.Type;
import com.google.zetasql.TypeFactory;
import com.google.zetasql.ZetaSQLType.TypeKind;
import com.google.zetasql.toolkit.catalog.typeparser.ZetaSQLTypeGrammarParser.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ConsoleErrorListener;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

/**
 * Parser for ZetaSQL types.
 *
 * <p>Allows parsing string representations of SQL types to their corresponding Type objects. For
 * example; it can parse type strings such as "STRING", "ARRAY&lt;INT64&gt;" and "STRUCT&lt;f
 * DECIMAL&gt;".
 *
 * <p>Uses an ANTLR4 based parser.
 */
public class ZetaSQLTypeParser {

  /**
   * Parses a SQL type string into its corresponding ZetaSQL Type.
   *
   * @param type The type string to parse
   * @return The corresponding ZetaSQL Type
   * @throws ZetaSQLTypeParseError if the provided type string is invalid
   */
  public static Type parse(String type) {
    Lexer lexer = new ZetaSQLTypeGrammarLexer(CharStreams.fromString(type));
    lexer.removeErrorListener(ConsoleErrorListener.INSTANCE);
    CommonTokenStream tokenStream = new CommonTokenStream(lexer);
    ZetaSQLTypeGrammarParser parser = new ZetaSQLTypeGrammarParser(tokenStream);
    ZetaSQLTypeParserListener listener = new ZetaSQLTypeParserListener();
    parser.removeErrorListener(ConsoleErrorListener.INSTANCE);
    parser.setErrorHandler(new BailErrorStrategy());

    try {
      TypeContext typeRule = parser.type();

      if (typeRule.exception != null) {
        throw new ZetaSQLTypeParseError(
            String.format("Invalid SQL type: %s", type), typeRule.exception);
      }

      ParseTreeWalker.DEFAULT.walk(listener, typeRule);

      return listener.getResult();
    } catch (ParseCancellationException err) {
      throw new ZetaSQLTypeParseError(String.format("Invalid SQL type: %s", type), err);
    }
  }

  /**
   * ANTLR4 listener that traverses the type parse tree and builds the corresponding ZetaSQL Type
   */
  private static class ZetaSQLTypeParserListener extends ZetaSQLTypeGrammarBaseListener {

    private static final Map<String, TypeKind> simpleTypeMapping = new HashMap<>();
    private final Stack<Type> typeStack = new Stack<>();
    private final Stack<List<StructField>> structFieldStack = new Stack<>();

    static {
      simpleTypeMapping.put("STRING", TypeKind.TYPE_STRING);
      simpleTypeMapping.put("BYTES", TypeKind.TYPE_BYTES);
      simpleTypeMapping.put("INT32", TypeKind.TYPE_INT32);
      simpleTypeMapping.put("INT64", TypeKind.TYPE_INT64);
      simpleTypeMapping.put("UINT32", TypeKind.TYPE_UINT32);
      simpleTypeMapping.put("UINT64", TypeKind.TYPE_UINT64);
      simpleTypeMapping.put("FLOAT64", TypeKind.TYPE_DOUBLE);
      simpleTypeMapping.put("DECIMAL", TypeKind.TYPE_NUMERIC);
      simpleTypeMapping.put("NUMERIC", TypeKind.TYPE_NUMERIC);
      simpleTypeMapping.put("BIGNUMERIC", TypeKind.TYPE_BIGNUMERIC);
      simpleTypeMapping.put("INTERVAL", TypeKind.TYPE_INTERVAL);
      simpleTypeMapping.put("BOOL", TypeKind.TYPE_BOOL);
      simpleTypeMapping.put("TIMESTAMP", TypeKind.TYPE_TIMESTAMP);
      simpleTypeMapping.put("DATE", TypeKind.TYPE_DATE);
      simpleTypeMapping.put("TIME", TypeKind.TYPE_TIME);
      simpleTypeMapping.put("DATETIME", TypeKind.TYPE_DATETIME);
      simpleTypeMapping.put("GEOGRAPHY", TypeKind.TYPE_GEOGRAPHY);
      simpleTypeMapping.put("JSON", TypeKind.TYPE_JSON);
    }

    public Type getResult() {
      return this.typeStack.pop();
    }

    @Override
    public void exitBasicType(BasicTypeContext ctx) {
      String basicTypeName = ctx.BASIC_TYPE().getText().toUpperCase();
      TypeKind kind = simpleTypeMapping.getOrDefault(basicTypeName, TypeKind.TYPE_UNKNOWN);
      Type type = TypeFactory.createSimpleType(kind);
      this.typeStack.push(type);
    }

    @Override
    public void exitArrayType(ArrayTypeContext ctx) {
      Type elementType = this.typeStack.pop();
      Type type = TypeFactory.createArrayType(elementType);
      this.typeStack.push(type);
    }

    @Override
    public void enterStructType(StructTypeContext ctx) {
      this.structFieldStack.add(new ArrayList<>());
    }

    @Override
    public void exitStructField(StructFieldContext ctx) {
      String fieldName = ctx.IDENTIFIER().getText();
      Type fieldType = this.typeStack.pop();
      StructField field = new StructField(fieldName, fieldType);
      this.structFieldStack.peek().add(field);
    }

    @Override
    public void exitStructType(StructTypeContext ctx) {
      if (this.structFieldStack.empty()) {
        return;
      }

      List<StructField> fields = this.structFieldStack.pop();
      Type type = TypeFactory.createStructType(fields);
      this.typeStack.push(type);
    }
  }
}
