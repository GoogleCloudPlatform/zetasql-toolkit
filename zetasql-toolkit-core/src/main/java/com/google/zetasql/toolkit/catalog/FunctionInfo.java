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

package com.google.zetasql.toolkit.catalog;

import com.google.common.base.Preconditions;
import com.google.zetasql.FunctionSignature;
import com.google.zetasql.ZetaSQLFunctions.FunctionEnums.Mode;
import java.util.List;
import java.util.Optional;

public class FunctionInfo {

  public enum Language {
    LANGUAGE_UNSPECIFIED, SQL;

    public static Language valueOfOrUnspecified(String name) {
      try {
        return Language.valueOf(name);
      } catch (NullPointerException | IllegalArgumentException err) {
        return LANGUAGE_UNSPECIFIED;
      }
    }
  }

  private final List<String> namePath;

  private final String group;

  private final Mode mode;

  private final List<FunctionSignature> signatures;

  private final Language language;

  private final String body;

  private FunctionInfo(Builder builder) {
    this.namePath = builder.getNamePath();
    this.group = builder.getGroup();
    this.mode = builder.getMode();
    this.signatures = builder.getSignatures();
    this.language = builder.getLanguage().orElse(null);
    this.body = builder.getBody().orElse(null);
  }

  public List<String> getNamePath() {
    return namePath;
  }

  public String getGroup() {
    return group;
  }

  public Mode getMode() {
    return mode;
  }

  public List<FunctionSignature> getSignatures() {
    return signatures;
  }

  public Optional<Language> getLanguage() {
    return Optional.ofNullable(this.language);
  }

  public Optional<String> getBody() {
    return Optional.ofNullable(this.body);
  }

  public Builder toBuilder() {
    return newBuilder()
        .setNamePath(this.namePath)
        .setGroup(this.group)
        .setMode(this.mode)
        .setSignatures(this.signatures)
        .setLanguage(this.getLanguage())
        .setBody(this.getBody());
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private List<String> namePath;
    private String group;
    private Mode mode;
    private List<FunctionSignature> signatures;
    private Optional<Language> language = Optional.empty();
    private Optional<String> body = Optional.empty();

    public Builder setNamePath(List<String> namePath) {
      this.namePath = namePath;
      return this;
    }

    public Builder setGroup(String group) {
      this.group = group;
      return this;
    }

    public Builder setMode(Mode mode) {
      this.mode = mode;
      return this;
    }

    public Builder setSignatures(List<FunctionSignature> signatures) {
      this.signatures = signatures;
      return this;
    }

    public Builder setLanguage(Language language) {
      Preconditions.checkNotNull(language);
      this.language = Optional.of(language);
      return this;
    }

    public Builder setLanguage(Optional<Language> language) {
      this.language = language;
      return this;
    }

    public Builder setBody(String body) {
      Preconditions.checkNotNull(body);
      this.body = Optional.of(body);
      return this;
    }

    public Builder setBody(Optional<String> body) {
      this.body = body;
      return this;
    }

    public List<String> getNamePath() {
      return namePath;
    }

    public String getGroup() {
      return group;
    }

    public Mode getMode() {
      return mode;
    }

    public List<FunctionSignature> getSignatures() {
      return signatures;
    }

    public Optional<Language> getLanguage() {
      return language;
    }

    public Optional<String> getBody() {
      return body;
    }

    public FunctionInfo build() {
      return new FunctionInfo(this);
    }
  }
}
