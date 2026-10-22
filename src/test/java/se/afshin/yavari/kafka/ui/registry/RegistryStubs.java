package se.afshin.yavari.kafka.ui.registry;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

import com.github.tomakehurst.wiremock.WireMockServer;

/** Canned Apicurio v3 responses shared by registry tests. */
final class RegistryStubs {

    static final String BASE = "/apis/registry/v3";
    static final String AVRO =
            "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}";

    private RegistryStubs() {}

    /** contentId 7 / globalId 11 -> AVRO schema order-value version 3, no references. */
    static void schema(WireMockServer s) {
        s.stubFor(get(urlEqualTo(BASE + "/ids/contentIds/7")).willReturn(ok(AVRO)));
        s.stubFor(get(urlEqualTo(BASE + "/ids/globalIds/11")).willReturn(ok(AVRO)));
        String meta = "{\"versions\":[{\"groupId\":\"default\",\"artifactId\":\"order-value\",\"version\":\"3\","
                + "\"artifactType\":\"AVRO\",\"globalId\":11,\"contentId\":7}],\"count\":1}";
        s.stubFor(get(urlEqualTo(BASE + "/search/versions?contentId=7")).willReturn(okJson(meta)));
        s.stubFor(get(urlEqualTo(BASE + "/search/versions?globalId=11")).willReturn(okJson(meta)));
        s.stubFor(get(urlEqualTo(BASE + "/ids/contentIds/7/references")).willReturn(okJson("[]")));
        s.stubFor(get(urlEqualTo(BASE + "/ids/globalIds/11/references")).willReturn(okJson("[]")));
    }
}
