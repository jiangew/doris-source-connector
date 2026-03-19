package org.apache.kafka.connect.doris.source.connector.fetcher;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class IdentifierEscaperTest {

    @Test
    public void escapesSimpleIdentifier() {
        IdentifierEscaper escaper = new IdentifierEscaper();
        assertEquals("`tbl`", escaper.escape("tbl"));
    }

    @Test
    public void escapesQualifiedIdentifier() {
        IdentifierEscaper escaper = new IdentifierEscaper();
        assertEquals("`db`.`table`", escaper.escape("db.table"));
    }

    @Test
    public void escapesBackticks() {
        IdentifierEscaper escaper = new IdentifierEscaper();
        assertEquals("`a``b`", escaper.escape("a`b"));
    }

    @Test
    public void rejectsEmptyIdentifier() {
        IdentifierEscaper escaper = new IdentifierEscaper();
        assertThrows(IllegalArgumentException.class, () -> escaper.escape(" "));
    }
}
