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
package io.tabular.iceberg.connect.data;

import static java.util.stream.Collectors.toList;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.tabular.iceberg.connect.IcebergSinkConfig;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.Temporal;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.mapping.MappedField;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.mapping.NameMappingParser;
import org.apache.iceberg.relocated.com.google.common.base.Preconditions;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Type.PrimitiveType;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.DecimalType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.TimestampType;
import org.apache.iceberg.util.DateTimeUtil;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;

public class RecordConverter {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final DateTimeFormatter OFFSET_TS_FMT =
      new DateTimeFormatterBuilder()
          .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
          .appendOffset("+HHmm", "Z")
          .toFormatter();

  private final Schema tableSchema;
  private final Set<Integer> partitionSourceIds = Sets.newHashSet();
  private final Map<org.apache.kafka.connect.data.Schema, Set<Integer>> sourceFieldIds =
      Maps.newHashMap();
  private final NameMapping nameMapping;
  private final IcebergSinkConfig config;
  private final Set<Integer> identifierFieldIds;
  private final Map<Integer, Map<String, NestedField>> structNameMap = Maps.newHashMap();
  private final boolean writeBeforeImageToIceberg;

  public RecordConverter(Table table, IcebergSinkConfig config) {
    this(table, config, false);
  }

  public RecordConverter(Table table, IcebergSinkConfig config, boolean writeBeforeImageToIceberg) {
    this.tableSchema = table.schema();
    if (table.specs() != null) {
      table
          .specs()
          .values()
          .forEach(
              spec ->
                  spec.fields()
                      .forEach(
                          field -> {
                            if (!field.transform().isVoid()) {
                              partitionSourceIds.add(field.sourceId());
                            }
                          }));
    }
    this.nameMapping = createNameMapping(table);
    this.config = config;
    this.identifierFieldIds = table.schema().identifierFieldIds();
    this.writeBeforeImageToIceberg = writeBeforeImageToIceberg;
  }

  public Record convert(Object data) {
    return convert(data, null);
  }

  public Record convert(Object data, SchemaUpdate.Consumer schemaUpdateConsumer) {
    if (data instanceof Struct || data instanceof Map) {
      Record row = convertStructValue(data, tableSchema.asStruct(), -1, schemaUpdateConsumer);
      if (identifierFieldIds.isEmpty()
          && (config.tablesCdcField() != null || config.upsertModeEnabled())) {
        return new RecordWrapper(row, Operation.INSERT, null, sourceFieldIds(data));
      }
      return row;
    }
    throw new UnsupportedOperationException("Cannot convert type: " + data.getClass().getName());
  }

  private boolean containsPartitionSource(NestedField field) {
    if (partitionSourceIds.contains(field.fieldId())) {
      return true;
    }
    return field.type().isStructType()
        && field.type().asStructType().fields().stream().anyMatch(this::containsPartitionSource);
  }

  // Field presence is taken from the source schema, even when a nested struct is null.
  // Schemaless records use map membership: an explicit null still participates in equality.
  private Set<Integer> sourceFieldIds(Object data) {
    if (data instanceof Struct) {
      org.apache.kafka.connect.data.Schema source = ((Struct) data).schema();
      if (sourceFieldIds.size() >= 64) {
        sourceFieldIds.clear();
      }
      return sourceFieldIds.computeIfAbsent(source, ignored -> collectSourceFieldIds(source, null));
    }
    return collectSourceFieldIds(null, data);
  }

  private Set<Integer> collectSourceFieldIds(
      org.apache.kafka.connect.data.Schema source, Object data) {
    Set<Integer> ids = Sets.newHashSet();
    collectSourceFields(source, data, tableSchema.asStruct(), -1, ids);
    return Set.copyOf(ids);
  }

  private void collectSourceFields(
      org.apache.kafka.connect.data.Schema source,
      Object data,
      StructType target,
      int parentId,
      Set<Integer> ids) {
    if (source != null) {
      for (Field field : source.fields()) {
        collectSourceField(field.name(), field.schema(), null, target, parentId, ids);
      }
    } else if (data instanceof Map) {
      ((Map<?, ?>) data)
          .forEach(
              (name, value) ->
                  collectSourceField(name.toString(), null, value, target, parentId, ids));
    } else {
      // A present, null schemaless struct means null for all of its children.
      for (NestedField field : target.fields()) {
        collectSourceField(field.name(), null, null, target, parentId, ids);
      }
    }
  }

  private void collectSourceField(
      String name,
      org.apache.kafka.connect.data.Schema source,
      Object data,
      StructType target,
      int parentId,
      Set<Integer> ids) {
    if (name.equals("_before_image")) {
      return;
    }
    NestedField field = lookupStructField(name, target, parentId);
    if (field == null) {
      return;
    }
    if (field.type().isStructType()) {
      collectSourceFields(source, data, field.type().asStructType(), field.fieldId(), ids);
    } else if (field.type().isPrimitiveType()) {
      ids.add(field.fieldId());
    }
  }

  /** Plan all changes from the schema, including null structs and empty collections. */
  public SchemaUpdate.Consumer planSchema(
      org.apache.kafka.connect.data.Schema source, boolean cdcWithoutKey) {
    if (source.type() != org.apache.kafka.connect.data.Schema.Type.STRUCT) {
      throw new ConnectException("Expected a struct schema for an Iceberg row");
    }
    SchemaUpdate.Consumer updates = new SchemaUpdate.Consumer();
    planStruct(source, tableSchema.asStruct(), -1, null, cdcWithoutKey, updates);
    if (cdcWithoutKey
        && Sets.intersection(
                collectSourceFieldIds(source, null),
                Utilities.collectEqualityDeleteFieldIds(tableSchema, "schema preflight"))
            .isEmpty()) {
      throw new ConnectException("Cannot DROP all equality fields in CDC without a primary key");
    }
    return updates;
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private void planStruct(
      org.apache.kafka.connect.data.Schema source,
      StructType target,
      int parentId,
      String parentPath,
      boolean cdcWithoutKey,
      SchemaUpdate.Consumer updates) {
    Set<Integer> present = Sets.newHashSet();
    for (Field field : source.fields()) {
      String name = field.name();
      if ("_before_image".equals(name)) {
        if (!writeBeforeImageToIceberg) {
          continue;
        }
        name = "_cdc_before_image";
      }
      String path = parentPath == null ? name : parentPath + "." + name;
      boolean protectedShape = cdcWithoutKey && !name.equals("_cdc") && !name.startsWith("_cdc_");
      NestedField existing = lookupStructField(name, target, parentId);
      if (existing == null) {
        if (protectedShape) {
          throw new ConnectException("Cannot ADD " + path + " in CDC without a primary key");
        }
        updates.addColumn(parentPath, name, SchemaUtils.toIcebergType(field.schema(), config));
      } else {
        present.add(existing.fieldId());
        String actualPath = tableSchema.findColumnName(existing.fieldId());
        planField(field.schema(), existing, actualPath, protectedShape, updates);
      }
    }
    for (NestedField existing : target.fields()) {
      // CDC metadata and before images are absent in some operations by design.
      if (!present.contains(existing.fieldId())
          && !existing.name().equals("_cdc")
          && !existing.name().startsWith("_cdc_")) {
        String path = tableSchema.findColumnName(existing.fieldId());
        if (containsIdentifier(existing.type(), existing.fieldId())) {
          throw new ConnectException("Cannot DROP identifier field " + path);
        }
        if (cdcWithoutKey && containsPartitionSource(existing)) {
          throw new ConnectException(
              "Cannot DROP partition source field " + path + " in CDC without a primary key");
        }
        if (existing.isRequired()) {
          updates.makeOptional(path);
        }
      }
    }
  }

  private boolean containsIdentifier(Type type, int fieldId) {
    if (identifierFieldIds.contains(fieldId)) {
      return true;
    }
    if (type.isStructType()) {
      for (NestedField child : type.asStructType().fields()) {
        if (containsIdentifier(child.type(), child.fieldId())) {
          return true;
        }
      }
    }
    return false;
  }

  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  private void planField(
      org.apache.kafka.connect.data.Schema source,
      NestedField target,
      String path,
      boolean cdcWithoutKey,
      SchemaUpdate.Consumer updates) {
    if (source.isOptional() && containsIdentifier(target.type(), target.fieldId())) {
      throw new ConnectException("Cannot make identifier field nullable: " + path);
    }
    if (source.isOptional() && target.isRequired()) {
      updates.makeOptional(path);
    }
    Type incoming = SchemaUtils.toIcebergType(source, config);
    Type current = target.type();
    if (incoming.isStructType() && current.isStructType()) {
      planStruct(source, current.asStructType(), target.fieldId(), path, cdcWithoutKey, updates);
    } else if (incoming.isListType() && current.isListType()) {
      planField(
          source.valueSchema(),
          current.asListType().fields().get(0),
          path + ".element",
          cdcWithoutKey,
          updates);
    } else if (incoming.isMapType() && current.isMapType()) {
      if (!sameTypeIgnoringIds(incoming.asMapType().keyType(), current.asMapType().keyType())) {
        throw incompatible(
            path + ".key", incoming.asMapType().keyType(), current.asMapType().keyType());
      }
      planField(
          source.valueSchema(),
          current.asMapType().fields().get(1),
          path + ".value",
          cdcWithoutKey,
          updates);
    } else if (incoming.isPrimitiveType() && current.isPrimitiveType()) {
      if (TypeUtil.isPromotionAllowed(incoming, current.asPrimitiveType())) {
        return;
      }
      if (TypeUtil.isPromotionAllowed(current, incoming.asPrimitiveType())) {
        updates.updateType(path, incoming.asPrimitiveType());
      } else {
        throw incompatible(path, incoming, current);
      }
    } else {
      throw incompatible(path, incoming, current);
    }
  }

  private static boolean sameTypeIgnoringIds(Type left, Type right) {
    if (left.typeId() != right.typeId()) {
      return false;
    }
    if (left.isPrimitiveType()) {
      return left.equals(right);
    }
    List<NestedField> leftFields = left.asNestedType().fields();
    List<NestedField> rightFields = right.asNestedType().fields();
    if (leftFields.size() != rightFields.size()) {
      return false;
    }
    for (int i = 0; i < leftFields.size(); i++) {
      if (!leftFields.get(i).name().equals(rightFields.get(i).name())
          || leftFields.get(i).isOptional() != rightFields.get(i).isOptional()
          || !sameTypeIgnoringIds(leftFields.get(i).type(), rightFields.get(i).type())) {
        return false;
      }
    }
    return true;
  }

  private static ConnectException incompatible(String path, Type source, Type target) {
    return new ConnectException(
        "Incompatible schema at "
            + path
            + ": incoming "
            + source
            + ", Iceberg "
            + target
            + "; no lossless Iceberg v2 promotion is available");
  }

  private NameMapping createNameMapping(Table table) {
    String nameMappingString = table.properties().get(TableProperties.DEFAULT_NAME_MAPPING);
    return nameMappingString != null ? NameMappingParser.fromJson(nameMappingString) : null;
  }

  private Object convertValue(
      Object value, Type type, int fieldId, SchemaUpdate.Consumer schemaUpdateConsumer) {
    return convertValue(value, type, fieldId, schemaUpdateConsumer, null);
  }

  private Object convertValue(
      Object value,
      Type type,
      int fieldId,
      SchemaUpdate.Consumer schemaUpdateConsumer,
      org.apache.kafka.connect.data.Schema sourceSchema) {
    if (value == null) {
      return null;
    }
    String sourceSchemaName = sourceSchema != null ? sourceSchema.name() : null;
    switch (type.typeId()) {
      case STRUCT:
        return convertStructValue(value, type.asStructType(), fieldId, schemaUpdateConsumer);
      case LIST:
        return convertListValue(value, type.asListType(), schemaUpdateConsumer, sourceSchema);
      case MAP:
        return convertMapValue(value, type.asMapType(), schemaUpdateConsumer, sourceSchema);
      case INTEGER:
        return convertInt(value);
      case LONG:
        return convertLong(value);
      case FLOAT:
        return convertFloat(value);
      case DOUBLE:
        return convertDouble(value);
      case DECIMAL:
        return convertDecimal(value, (Types.DecimalType) type);
      case BOOLEAN:
        return convertBoolean(value);
      case STRING:
        if (config.schemaVariableDecimalAsString()
            && "io.debezium.data.VariableScaleDecimal".equals(sourceSchemaName)) {
          Struct decimal = (Struct) value;
          Object bytes = decimal.get("value");
          byte[] unscaled;
          if (bytes instanceof ByteBuffer) {
            ByteBuffer buffer = ((ByteBuffer) bytes).duplicate();
            unscaled = new byte[buffer.remaining()];
            buffer.get(unscaled);
          } else {
            unscaled = (byte[]) bytes;
          }
          return new BigDecimal(new BigInteger(unscaled), decimal.getInt32("scale"))
              .toPlainString();
        }
        return convertString(value);
      case UUID:
        return convertUUID(value);
      case BINARY:
      case FIXED:
        return convertBase64Binary(value);
      case DATE:
        return convertDateValue(value);
      case TIME:
        return convertTimeValue(value, sourceSchemaName);
      case TIMESTAMP:
        return convertTimestampValue(value, (TimestampType) type, sourceSchemaName);
    }
    throw new UnsupportedOperationException("Unsupported type: " + type.typeId());
  }

  protected GenericRecord convertStructValue(
      Object value,
      StructType schema,
      int parentFieldId,
      SchemaUpdate.Consumer schemaUpdateConsumer) {
    if (value instanceof Map) {
      return convertToStruct((Map<?, ?>) value, schema, parentFieldId, schemaUpdateConsumer);
    } else if (value instanceof Struct) {
      return convertToStruct((Struct) value, schema, parentFieldId, schemaUpdateConsumer);
    }
    throw new IllegalArgumentException("Cannot convert to struct: " + value.getClass().getName());
  }

  private GenericRecord convertToStruct(
      Map<?, ?> map,
      StructType schema,
      int structFieldId,
      SchemaUpdate.Consumer schemaUpdateConsumer) {
    // make required columns optional if needed and if schema evolution is on
    if (schemaUpdateConsumer != null) {
      for (NestedField tableField : schema.fields()) {
        if (tableField.isRequired()) {
          if (!map.containsKey(tableField.name())) {
            // Don't make identifier fields optional
            if (identifierFieldIds != null && identifierFieldIds.contains(tableField.fieldId())) {
              // Skip making identifier fields optional - they must remain required
              continue;
            }
            String fieldName = tableSchema.findColumnName(tableField.fieldId());
            schemaUpdateConsumer.makeOptional(fieldName);
          }
        }
      }
    }

    GenericRecord result = GenericRecord.create(schema);
    map.forEach(
        (recordFieldNameObj, recordFieldValue) -> {
          String recordFieldName = recordFieldNameObj.toString();

          // Handle _before_image field
          if ("_before_image".equals(recordFieldName)) {
            if (writeBeforeImageToIceberg) {
              // Rename to _cdc_before_image and write to Iceberg
              recordFieldName = "_cdc_before_image";
            } else {
              // Skip - it's only used for delete logic, not stored in Iceberg
              return;
            }
          }

          NestedField tableField = lookupStructField(recordFieldName, schema, structFieldId);
          if (tableField == null) {
            // add the column if schema evolution is on, otherwise skip the value,
            // skip the add column if we can't infer the type
            if (schemaUpdateConsumer != null) {
              Optional<Type> type = SchemaUtils.inferIcebergType(recordFieldValue, config);
              if (type.isPresent()) {
                String parentFieldName =
                    structFieldId < 0 ? null : tableSchema.findColumnName(structFieldId);
                schemaUpdateConsumer.addColumn(parentFieldName, recordFieldName, type.get());
              }
            }
          } else {
            result.setField(
                tableField.name(),
                convertValue(
                    recordFieldValue,
                    tableField.type(),
                    tableField.fieldId(),
                    schemaUpdateConsumer,
                    null));
          }
        });
    return result;
  }

  private GenericRecord convertToStruct(
      Struct struct,
      StructType schema,
      int structFieldId,
      SchemaUpdate.Consumer schemaUpdateConsumer) {
    boolean hasSchemaUpdates = false;
    // make required columns optional if needed and if schema evolution is on
    if (schemaUpdateConsumer != null) {
      for (NestedField tableField : schema.fields()) {
        if (tableField.isRequired()) {
          Field recordField = struct.schema().field(tableField.name());
          if (recordField == null) {
            // Don't make identifier fields optional
            if (identifierFieldIds != null && identifierFieldIds.contains(tableField.fieldId())) {
              continue;
            }
            hasSchemaUpdates = true;
            String fieldName = tableSchema.findColumnName(tableField.fieldId());
            schemaUpdateConsumer.makeOptional(fieldName);
          }
        }
      }
    }

    GenericRecord result = GenericRecord.create(schema);

    for (Field recordField : struct.schema().fields()) {
      String recordFieldName = recordField.name();

      // Handle _before_image field
      if ("_before_image".equals(recordFieldName)) {
        if (writeBeforeImageToIceberg) {
          // Rename to _cdc_before_image and write to Iceberg
          recordFieldName = "_cdc_before_image";
        } else {
          // Skip - it's only used for delete logic, not stored in Iceberg
          continue;
        }
      }

      NestedField tableField = lookupStructField(recordFieldName, schema, structFieldId);
      if (tableField == null) {
        // add the column if schema evolution is on, otherwise skip the value
        if (schemaUpdateConsumer != null) {
          String parentFieldName =
              structFieldId < 0 ? null : tableSchema.findColumnName(structFieldId);
          Type type = SchemaUtils.toIcebergType(recordField.schema(), config);
          schemaUpdateConsumer.addColumn(parentFieldName, recordFieldName, type);
        }
      } else {
        if (schemaUpdateConsumer != null) {
          // update the type if needed and schema evolution is on
          PrimitiveType evolveDataType =
              SchemaUtils.needsDataTypeUpdate(tableField.type(), recordField.schema());
          if (evolveDataType != null) {
            String fieldName = tableSchema.findColumnName(tableField.fieldId());
            schemaUpdateConsumer.updateType(fieldName, evolveDataType);
            hasSchemaUpdates = true;
          }

          // make optional if needed and schema evolution is on
          if (tableField.isRequired() && recordField.schema().isOptional()) {
            // Don't make identifier fields optional
            if (identifierFieldIds != null && identifierFieldIds.contains(tableField.fieldId())) {
            } else {
              String fieldName = tableSchema.findColumnName(tableField.fieldId());
              schemaUpdateConsumer.makeOptional(fieldName);
              hasSchemaUpdates = true;
            }
          }
        }

        if (!hasSchemaUpdates) {
          result.setField(
              tableField.name(),
              convertValue(
                  struct.get(recordField),
                  tableField.type(),
                  tableField.fieldId(),
                  schemaUpdateConsumer,
                  recordField.schema()));
        }
      }
    }

    return result;
  }

  private NestedField lookupStructField(String fieldName, StructType schema, int structFieldId) {
    if (nameMapping == null) {
      return config.schemaCaseInsensitive()
          ? schema.caseInsensitiveField(fieldName)
          : schema.field(fieldName);
    }

    return structNameMap
        .computeIfAbsent(structFieldId, notUsed -> createStructNameMap(schema))
        .get(fieldName);
  }

  private Map<String, NestedField> createStructNameMap(StructType schema) {
    Map<String, NestedField> map = Maps.newHashMap();
    schema
        .fields()
        .forEach(
            col -> {
              MappedField mappedField = nameMapping.find(col.fieldId());
              if (mappedField != null && !mappedField.names().isEmpty()) {
                mappedField.names().forEach(name -> map.put(name, col));
              } else {
                map.put(col.name(), col);
              }
            });
    return map;
  }

  protected List<Object> convertListValue(
      Object value,
      ListType type,
      SchemaUpdate.Consumer schemaUpdateConsumer,
      org.apache.kafka.connect.data.Schema sourceSchema) {
    Preconditions.checkArgument(value instanceof List);
    List<?> list = (List<?>) value;
    org.apache.kafka.connect.data.Schema elementSchema =
        sourceSchema != null ? sourceSchema.valueSchema() : null;
    return list.stream()
        .map(
            element -> {
              int fieldId = type.fields().get(0).fieldId();
              return convertValue(
                  element, type.elementType(), fieldId, schemaUpdateConsumer, elementSchema);
            })
        .collect(toList());
  }

  protected Map<Object, Object> convertMapValue(
      Object value,
      MapType type,
      SchemaUpdate.Consumer schemaUpdateConsumer,
      org.apache.kafka.connect.data.Schema sourceSchema) {
    Preconditions.checkArgument(value instanceof Map);
    Map<?, ?> map = (Map<?, ?>) value;
    Map<Object, Object> result = Maps.newHashMap();
    org.apache.kafka.connect.data.Schema keySchema =
        sourceSchema != null ? sourceSchema.keySchema() : null;
    org.apache.kafka.connect.data.Schema valueSchema =
        sourceSchema != null ? sourceSchema.valueSchema() : null;
    map.forEach(
        (k, v) -> {
          int keyFieldId = type.fields().get(0).fieldId();
          int valueFieldId = type.fields().get(1).fieldId();
          result.put(
              convertValue(k, type.keyType(), keyFieldId, schemaUpdateConsumer, keySchema),
              convertValue(v, type.valueType(), valueFieldId, schemaUpdateConsumer, valueSchema));
        });
    return result;
  }

  protected int convertInt(Object value) {
    if (value instanceof Number) {
      return ((Number) value).intValue();
    } else if (value instanceof String) {
      return Integer.parseInt((String) value);
    }
    throw new IllegalArgumentException("Cannot convert to int: " + value.getClass().getName());
  }

  protected long convertLong(Object value) {
    if (value instanceof Number) {
      return ((Number) value).longValue();
    } else if (value instanceof String) {
      return Long.parseLong((String) value);
    }
    throw new IllegalArgumentException("Cannot convert to long: " + value.getClass().getName());
  }

  protected float convertFloat(Object value) {
    if (value instanceof Number) {
      return ((Number) value).floatValue();
    } else if (value instanceof String) {
      return Float.parseFloat((String) value);
    }
    throw new IllegalArgumentException("Cannot convert to float: " + value.getClass().getName());
  }

  protected double convertDouble(Object value) {
    if (value instanceof Number) {
      return ((Number) value).doubleValue();
    } else if (value instanceof String) {
      return Double.parseDouble((String) value);
    }
    throw new IllegalArgumentException("Cannot convert to double: " + value.getClass().getName());
  }

  protected BigDecimal convertDecimal(Object value, DecimalType type) {
    BigDecimal bigDecimal;
    if (value instanceof BigDecimal) {
      bigDecimal = (BigDecimal) value;
    } else if (value instanceof Number) {
      Number num = (Number) value;
      Double dbl = num.doubleValue();
      if (dbl.equals(Math.floor(dbl))) {
        bigDecimal = BigDecimal.valueOf(num.longValue());
      } else {
        bigDecimal = BigDecimal.valueOf(dbl);
      }
    } else if (value instanceof String) {
      bigDecimal = new BigDecimal((String) value);
    } else {
      throw new IllegalArgumentException(
          "Cannot convert to BigDecimal: " + value.getClass().getName());
    }
    BigDecimal scaled = bigDecimal.setScale(type.scale(), RoundingMode.UNNECESSARY);
    if (scaled.precision() > type.precision()) {
      throw new ConnectException("Decimal value exceeds target " + type);
    }
    return scaled;
  }

  protected boolean convertBoolean(Object value) {
    if (value instanceof Boolean) {
      return (boolean) value;
    } else if (value instanceof String) {
      return Boolean.parseBoolean((String) value);
    }
    throw new IllegalArgumentException("Cannot convert to boolean: " + value.getClass().getName());
  }

  protected String convertString(Object value) {
    try {
      if (value instanceof String) {
        return (String) value;
      } else if (value instanceof Number || value instanceof Boolean) {
        return value.toString();
      } else if (value instanceof Map || value instanceof List) {
        return MAPPER.writeValueAsString(value);
      } else if (value instanceof Struct) {
        Struct struct = (Struct) value;
        byte[] data = config.jsonConverter().fromConnectData(null, struct.schema(), struct);
        return new String(data, StandardCharsets.UTF_8);
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    throw new IllegalArgumentException("Cannot convert to string: " + value.getClass().getName());
  }

  protected UUID convertUUID(Object value) {
    if (value instanceof String) {
      return UUID.fromString((String) value);
    } else if (value instanceof UUID) {
      return (UUID) value;
    }
    throw new IllegalArgumentException("Cannot convert to UUID: " + value.getClass().getName());
  }

  protected ByteBuffer convertBase64Binary(Object value) {
    if (value instanceof String) {
      return ByteBuffer.wrap(Base64.getDecoder().decode((String) value));
    } else if (value instanceof byte[]) {
      return ByteBuffer.wrap((byte[]) value);
    } else if (value instanceof ByteBuffer) {
      return (ByteBuffer) value;
    }
    throw new IllegalArgumentException("Cannot convert to binary: " + value.getClass().getName());
  }

  protected LocalDate convertDateValue(Object value) {
    if (value instanceof Number) {
      int days = ((Number) value).intValue();
      return DateTimeUtil.dateFromDays(days);
    } else if (value instanceof String) {
      return LocalDate.parse((String) value);
    } else if (value instanceof LocalDate) {
      return (LocalDate) value;
    } else if (value instanceof Date) {
      int days = (int) (((Date) value).getTime() / 1000 / 60 / 60 / 24);
      return DateTimeUtil.dateFromDays(days);
    }
    throw new ConnectException("Cannot convert date: " + value);
  }

  protected LocalTime convertTimeValue(Object value) {
    return convertTimeValue(value, null);
  }

  protected LocalTime convertTimeValue(Object value, String sourceSchemaName) {
    if (value instanceof Number) {
      long numValue = ((Number) value).longValue();
      long micros;
      if (config.schemaDebeziumTimeTypes() && sourceSchemaName != null) {
        switch (sourceSchemaName) {
          case "io.debezium.time.MicroTime":
            // Value is already in microseconds
            micros = numValue;
            break;
          case "io.debezium.time.NanoTime":
            // Convert nanoseconds to microseconds
            micros = numValue / 1000;
            break;
          default:
            // Default: assume milliseconds
            micros = numValue * 1000;
            break;
        }
      } else {
        // Default: assume milliseconds
        micros = numValue * 1000;
      }
      return DateTimeUtil.timeFromMicros(micros);
    } else if (value instanceof String) {
      String str = (String) value;
      if (config.schemaDebeziumTimeTypes()
          && "io.debezium.time.ZonedTime".equals(sourceSchemaName)) {
        try {
          return java.time.OffsetTime.parse(str).withOffsetSameInstant(ZoneOffset.UTC).toLocalTime();
        } catch (DateTimeParseException e) {
          // Fall through to LocalTime parsing
        }
      }
      return LocalTime.parse(str);
    } else if (value instanceof LocalTime) {
      return (LocalTime) value;
    } else if (value instanceof Date) {
      long millis = ((Date) value).getTime();
      return DateTimeUtil.timeFromMicros(millis * 1000);
    }
    throw new ConnectException("Cannot convert time: " + value);
  }

  protected Temporal convertTimestampValue(Object value, TimestampType type) {
    return convertTimestampValue(value, type, null);
  }

  protected Temporal convertTimestampValue(
      Object value, TimestampType type, String sourceSchemaName) {
    if (type.shouldAdjustToUTC()) {
      return convertOffsetDateTime(value, sourceSchemaName);
    }
    return convertLocalDateTime(value, sourceSchemaName);
  }

  private OffsetDateTime convertOffsetDateTime(Object value, String sourceSchemaName) {
    if (value instanceof Number) {
      long numValue = ((Number) value).longValue();
      long micros = convertToMicros(numValue, sourceSchemaName);
      return DateTimeUtil.timestamptzFromMicros(micros);
    } else if (value instanceof String) {
      return parseOffsetDateTime((String) value);
    } else if (value instanceof OffsetDateTime) {
      return (OffsetDateTime) value;
    } else if (value instanceof LocalDateTime) {
      return ((LocalDateTime) value).atOffset(ZoneOffset.UTC);
    } else if (value instanceof Date) {
      return DateTimeUtil.timestamptzFromMicros(((Date) value).getTime() * 1000);
    }
    throw new ConnectException(
        "Cannot convert timestamptz: " + value + ", type: " + value.getClass());
  }

  private OffsetDateTime parseOffsetDateTime(String str) {
    // Handle special PostgreSQL values
    if ("infinity".equals(str)) {
      return OffsetDateTime.MAX;
    } else if ("-infinity".equals(str)) {
      return OffsetDateTime.MIN;
    }

    String tsStr = ensureTimestampFormat(str);
    try {
      return OFFSET_TS_FMT.parse(tsStr, OffsetDateTime::from);
    } catch (DateTimeParseException e) {
      // Try ISO_OFFSET_DATE_TIME for formats like "2020-04-01T11:51:02.000000Z"
      try {
        return OffsetDateTime.parse(tsStr, DateTimeFormatter.ISO_OFFSET_DATE_TIME);
      } catch (DateTimeParseException e2) {
        return LocalDateTime.parse(tsStr, DateTimeFormatter.ISO_LOCAL_DATE_TIME)
            .atOffset(ZoneOffset.UTC);
      }
    }
  }

  private LocalDateTime convertLocalDateTime(Object value, String sourceSchemaName) {
    if (value instanceof Number) {
      long numValue = ((Number) value).longValue();
      long micros = convertToMicros(numValue, sourceSchemaName);
      return DateTimeUtil.timestampFromMicros(micros);
    } else if (value instanceof String) {
      return parseLocalDateTime((String) value);
    } else if (value instanceof LocalDateTime) {
      return (LocalDateTime) value;
    } else if (value instanceof OffsetDateTime) {
      return ((OffsetDateTime) value).toLocalDateTime();
    } else if (value instanceof Date) {
      return DateTimeUtil.timestampFromMicros(((Date) value).getTime() * 1000);
    }
    throw new ConnectException(
        "Cannot convert timestamp: " + value + ", type: " + value.getClass());
  }

  /**
   * Converts a numeric timestamp value to microseconds based on the source schema type.
   *
   * @param numValue the numeric value from the source
   * @param sourceSchemaName the Debezium/Connect schema name indicating the precision
   * @return the value in microseconds
   */
  private long convertToMicros(long numValue, String sourceSchemaName) {
    if (config.schemaDebeziumTimeTypes() && sourceSchemaName != null) {
      switch (sourceSchemaName) {
        case "io.debezium.time.MicroTimestamp":
          // Value is already in microseconds
          return numValue;
        case "io.debezium.time.NanoTimestamp":
          // Convert nanoseconds to microseconds
          return numValue / 1000;
        case "io.debezium.time.Timestamp":
          // Value is in milliseconds
          return numValue * 1000;
        default:
          break;
      }
    }
    // Default: assume milliseconds (Kafka Connect Timestamp behavior)
    return numValue * 1000;
  }

  private LocalDateTime parseLocalDateTime(String str) {
    String tsStr = ensureTimestampFormat(str);
    try {
      return LocalDateTime.parse(tsStr, DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    } catch (DateTimeParseException e) {
      return OFFSET_TS_FMT.parse(tsStr, OffsetDateTime::from).toLocalDateTime();
    }
  }

  private String ensureTimestampFormat(String str) {
    String result = str;
    if (result.charAt(10) == ' ') {
      result = result.substring(0, 10) + 'T' + result.substring(11);
    }
    if (result.length() > 22 && result.charAt(19) == '+' && result.charAt(22) == ':') {
      result = result.substring(0, 19) + result.substring(19).replace(":", "");
    }
    return result;
  }
}
