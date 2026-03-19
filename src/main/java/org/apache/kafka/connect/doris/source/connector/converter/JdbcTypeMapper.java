package org.apache.kafka.connect.doris.source.connector.converter;

import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Timestamp;

import java.math.BigDecimal;
import java.sql.Types;

public class JdbcTypeMapper {
    public Schema schemaFor(int sqlType, Object value) {
        switch (sqlType) {
            case Types.BIGINT:
                return SchemaBuilder.int64().optional().build();
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
                return SchemaBuilder.int32().optional().build();
            case Types.BOOLEAN:
            case Types.BIT:
                return SchemaBuilder.bool().optional().build();
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
                return SchemaBuilder.float64().optional().build();
            case Types.DECIMAL:
            case Types.NUMERIC:
                BigDecimal decimal = value instanceof BigDecimal ? (BigDecimal) value : null;
                int scale = decimal != null ? decimal.scale() : 0;
                return Decimal.builder(scale).optional().build();
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                return Timestamp.builder().optional().build();
            case Types.DATE:
                return Date.builder().optional().build();
            default:
                return SchemaBuilder.string().optional().build();
        }
    }
}
