package org.apache.kafka.connect.doris.source.connector.client;

import org.apache.kafka.connect.doris.source.connector.model.DorisRowWithTypes;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class DorisRowMapperTest {

    @Test
    public void mapsRowAndExtractsSeq() throws Exception {
        ResultSetMetaData meta = (ResultSetMetaData) Proxy.newProxyInstance(
                ResultSetMetaData.class.getClassLoader(),
                new Class<?>[]{ResultSetMetaData.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "getColumnCount":
                            return 2;
                        case "getColumnLabel":
                            return ((int) args[0] == 1) ? "sequence" : "name";
                        case "getColumnType":
                            return ((int) args[0] == 1) ? Types.BIGINT : Types.VARCHAR;
                        default:
                            throw new UnsupportedOperationException();
                    }
                }
        );

        Map<Integer, Object> values = new HashMap<>();
        values.put(1, 123L);
        values.put(2, "foo");

        ResultSet rs = (ResultSet) Proxy.newProxyInstance(
                ResultSet.class.getClassLoader(),
                new Class<?>[]{ResultSet.class},
                (proxy, method, args) -> {
                    switch (method.getName()) {
                        case "getMetaData":
                            return meta;
                        case "getObject":
                            return values.get((int) args[0]);
                        default:
                            throw new UnsupportedOperationException();
                    }
                }
        );

        DorisRowMapper mapper = new DorisRowMapper();
        DorisRowWithTypes row = mapper.map(rs, "sequence");

        assertEquals(123L, row.getSeq());
        assertEquals(123L, row.getData().get("sequence"));
        assertEquals("foo", row.getData().get("name"));
        assertEquals(Integer.valueOf(Types.BIGINT), row.getSqlTypes().get("sequence"));
        assertEquals(Integer.valueOf(Types.VARCHAR), row.getSqlTypes().get("name"));
    }
}
