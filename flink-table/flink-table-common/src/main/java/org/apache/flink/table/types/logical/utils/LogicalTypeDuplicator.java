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

package org.apache.flink.table.types.logical.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.RowType.RowField;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.StructuredType.StructuredAttribute;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Returns a deep copy of a {@link LogicalType}.
 *
 * <p>It also enables replacing children of possibly nested structures by overwriting corresponding
 * {@code visit()} methods.
 */
@Internal
public class LogicalTypeDuplicator extends LogicalTypeDefaultVisitor<LogicalType> {

    @Override
    public LogicalType visit(ArrayType arrayType) {
        return new ArrayType(arrayType.isNullable(), arrayType.getElementType().accept(this));
    }

    @Override
    public LogicalType visit(MultisetType multisetType) {
        return new MultisetType(
                multisetType.isNullable(), multisetType.getElementType().accept(this));
    }

    @Override
    public LogicalType visit(MapType mapType) {
        return new MapType(
                mapType.isNullable(),
                mapType.getKeyType().accept(this),
                mapType.getValueType().accept(this));
    }

    @Override
    public LogicalType visit(RowType rowType) {
        final List<RowField> fields =
                rowType.getFields().stream()
                        .map(
                                f -> {
                                    if (f.getDescription().isPresent()) {
                                        return new RowField(
                                                f.getName(),
                                                f.getType().accept(this),
                                                f.getDescription().get());
                                    }
                                    return new RowField(f.getName(), f.getType().accept(this));
                                })
                        .collect(Collectors.toList());

        return new RowType(rowType.isNullable(), fields);
    }

    @Override
    public LogicalType visit(DistinctType distinctType) {
        final DistinctType.Builder builder =
                DistinctType.newBuilder(
                        distinctType.getObjectIdentifier().orElseThrow(IllegalStateException::new),
                        distinctType.getSourceType().accept(this));
        distinctType.getDescription().ifPresent(builder::description);
        return builder.build();
    }

    @Override
    public LogicalType visit(StructuredType structuredType) {
        final StructuredType.Builder builder = createStructuredBuilder(structuredType);
        builder.attributes(duplicateStructuredAttributes(structuredType));
        builder.setNullable(structuredType.isNullable());
        builder.setFinal(structuredType.isFinal());
        builder.setInstantiable(structuredType.isInstantiable());
        builder.comparison(structuredType.getComparison());
        structuredType
                .getSuperType()
                .ifPresent(
                        st -> {
                            final LogicalType visited = st.accept(this);
                            if (!(visited instanceof StructuredType)) {
                                throw new TableException(
                                        "Unexpected super type. Structured type expected but was: "
                                                + visited);
                            }
                            builder.superType((StructuredType) visited);
                        });
        structuredType.getDescription().ifPresent(builder::description);
        return builder.build();
    }

    @Override
    protected LogicalType defaultMethod(LogicalType logicalType) {
        return logicalType.copy();
    }

    // --------------------------------------------------------------------------------------------

    private StructuredType.Builder createStructuredBuilder(StructuredType structuredType) {
        final ObjectIdentifier identifier = structuredType.getObjectIdentifier().orElse(null);
        final String className = structuredType.getClassName().orElse(null);
        final Class<?> implementationClass = structuredType.getImplementationClass().orElse(null);

        if (identifier != null && implementationClass != null) {
            return StructuredType.newBuilder(identifier, implementationClass);
        } else if (identifier != null) {
            return StructuredType.newBuilder(identifier);
        } else if (implementationClass != null) {
            return StructuredType.newBuilder(implementationClass);
        } else {
            return StructuredType.newBuilder(className);
        }
    }

    private List<StructuredAttribute> duplicateStructuredAttributes(StructuredType structuredType) {
        return structuredType.getAttributes().stream()
                .map(
                        a -> {
                            if (a.getDescription().isPresent()) {
                                return new StructuredAttribute(
                                        a.getName(),
                                        a.getType().accept(this),
                                        a.getDescription().get());
                            }
                            return new StructuredAttribute(a.getName(), a.getType().accept(this));
                        })
                .collect(Collectors.toList());
    }
}
