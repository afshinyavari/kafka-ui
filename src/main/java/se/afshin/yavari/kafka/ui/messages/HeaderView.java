package se.afshin.yavari.kafka.ui.messages;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import org.apache.kafka.common.header.Header;

/** A record header for display: UTF-8 text when valid, otherwise hex. */
public record HeaderView(String key, String value) {

    static HeaderView of(Header h) {
        byte[] v = h.value();
        if (v == null) {
            return new HeaderView(h.key(), "");
        }
        try {
            return new HeaderView(h.key(), StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(v)).toString());
        } catch (CharacterCodingException e) {
            return new HeaderView(h.key(), "0x" + HexFormat.of().formatHex(v));
        }
    }
}
