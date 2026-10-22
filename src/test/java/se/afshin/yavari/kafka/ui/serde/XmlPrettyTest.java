package se.afshin.yavari.kafka.ui.serde;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class XmlPrettyTest {

    @Test
    void indentsXml() {
        assertThat(XmlPretty.pretty("<a><b>1</b><c/></a>")).hasValue("<a>\n  <b>1</b>\n  <c/>\n</a>");
    }

    @Test
    void rejectsDoctypeAndNonXml() {
        assertThat(XmlPretty.pretty("<!DOCTYPE x [<!ENTITY e SYSTEM \"file:///etc/passwd\">]><x>&e;</x>")).isEmpty();
        assertThat(XmlPretty.pretty("hello")).isEmpty();
        assertThat(XmlPretty.pretty("<a><b></a>")).isEmpty();
    }

    @Test
    void deeplyNestedXmlIsRejectedWithoutStackOverflow() {
        String deep = "<a>".repeat(5000) + "</a>".repeat(5000);
        assertThat(XmlPretty.pretty(deep)).isEmpty();
        String fine = "<a>".repeat(200) + "</a>".repeat(200);
        assertThat(XmlPretty.pretty(fine)).isPresent();
    }
}
