/*
 * Copyright 2015-2018 Transmogrify LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pyranid;

import javax.annotation.Nonnull;
import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.NotThreadSafe;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * Application-specific metadata associated with a SQL statement.
 * <p>
 * This is used in conjunction with {@link StatementLog} - for example, you might want to mark a query as "hot", or should not be logged, or ...
 *
 * @author <a href="http://revetkn.com">Mark Allen</a>
 * @since 1.0.11
 */
@Immutable
public class StatementMetadata {
  @Nonnull
  private final Map<String, Object> metadata;

  public StatementMetadata() {
    this(new Builder());
  }

  private StatementMetadata(@Nonnull Builder builder) {
    requireNonNull(builder);
    this.metadata = Collections.unmodifiableMap(new HashMap<>(builder.getMetadata()));
  }

  @Nonnull
  public static StatementMetadata with(@Nonnull String key, @Nonnull Object value) {
    requireNonNull(key);
    requireNonNull(value);
    return new Builder().add(key, value).build();
  }

  @Nonnull
  public Optional<Object> get(@Nonnull String key) {
    requireNonNull(key);
    return Optional.ofNullable(getMetadata().get(key));
  }

  @Override
  public String toString() {
    return format("%s{metadata=%s}", getClass().getSimpleName(), getMetadata());
  }

  @Override
  public boolean equals(Object object) {
    if (this == object)
      return true;

    if (!(object instanceof StatementMetadata))
      return false;

    StatementMetadata statementMetadata = (StatementMetadata) object;

    return Objects.equals(getMetadata(), statementMetadata.getMetadata());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getMetadata());
  }

  @Nonnull
  private Map<String, Object> getMetadata() {
    return metadata;
  }

  @NotThreadSafe
  public static class Builder {
    @Nonnull
    private final Map<String, Object> metadata;

    public Builder() {
      this.metadata = new HashMap<>();
    }

    public Builder add(@Nonnull String key, @Nonnull Object value) {
      requireNonNull(key);
      requireNonNull(value);
      getMetadata().put(key, value);
      return this;
    }

    public Builder remove(@Nonnull String key) {
      requireNonNull(key);
      getMetadata().remove(key);
      return this;
    }

    @Nonnull
    public StatementMetadata build() {
      return new StatementMetadata(this);
    }

    @Nonnull
    private Map<String, Object> getMetadata() {
      return metadata;
    }
  }
}