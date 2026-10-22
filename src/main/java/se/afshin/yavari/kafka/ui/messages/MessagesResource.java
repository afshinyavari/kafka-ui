package se.afshin.yavari.kafka.ui.messages;

import io.quarkus.qute.CheckedTemplate;
import io.quarkus.qute.Location;
import io.quarkus.qute.Template;
import io.quarkus.qute.TemplateInstance;
import io.smallrye.mutiny.Multi;
import jakarta.inject.Inject;
import jakarta.ws.rs.BeanParam;
import jakarta.ws.rs.DefaultValue;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.sse.OutboundSseEvent;
import jakarta.ws.rs.sse.Sse;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import org.jboss.logging.Logger;
import se.afshin.yavari.kafka.ui.config.KafkaUiConfig;
import se.afshin.yavari.kafka.ui.kafka.KafkaErrors;
import se.afshin.yavari.kafka.ui.kafka.UserError;
import se.afshin.yavari.kafka.ui.kafka.UserErrorException;
import se.afshin.yavari.kafka.ui.security.Credentials;
import se.afshin.yavari.kafka.ui.security.CredentialsProvider;
import se.afshin.yavari.kafka.ui.serde.Format;
import se.afshin.yavari.kafka.ui.topics.TopicService;
import se.afshin.yavari.kafka.ui.web.Htmx;

/** The topic's Messages tab: search form, streamed search results and message detail. */
@Path("/topics/{name}/messages")
@Produces(MediaType.TEXT_HTML)
public class MessagesResource {

    private static final Logger LOG = Logger.getLogger(MessagesResource.class);

    /** Formats offered by "Decode as" (besides automatic). */
    static final List<Format> DECODE_AS = List.of(
            Format.AVRO, Format.JSON_SCHEMA, Format.JSON, Format.XML, Format.TEXT, Format.HEX);

    @CheckedTemplate
    static class Templates {
        static native TemplateInstance results(String name, SearchForm form, String error);

        static native TemplateInstance row(String topic, MessageView m);

        static native TemplateInstance progress(SearchEvent.Progress p);

        static native TemplateInstance done(SearchEvent.Done d);

        static native TemplateInstance failed(String message);

        static native TemplateInstance detail(String name, MessageView m, String keyAs, String valueAs,
                List<Format> formats);
    }

    /** Search form fields, shared by the results fragment and the event stream. */
    public static class SearchParams {
        @QueryParam("partition") List<Integer> partitions;
        @QueryParam("from") String from;
        @QueryParam("to") String to;
        @QueryParam("byOffset") @DefaultValue("false") boolean byOffset;
        @QueryParam("offset") String offset;
        @QueryParam("keyRegex") String keyRegex;
        @QueryParam("valueRegex") String valueRegex;
    }

    @Inject TopicService topics;
    @Inject MessageSearchService search;
    @Inject CredentialsProvider credentials;
    @Inject KafkaUiConfig config;

    @Inject @Location("TopicsResource/panel") Template panel;
    @Inject @Location("TopicsResource/detail") Template topicPage;

    @Context HttpHeaders headers;

    /** The tab with an empty form (last hour); htmx gets the tab bar plus panel, otherwise the full page. */
    @GET
    public TemplateInstance tab(@PathParam("name") String name) {
        var info = topics.describe(credentials.current(), name);
        return (Htmx.isFragment(headers) ? panel : topicPage).instance()
                .data("name", name)
                .data("tab", "messages")
                .data("info", info)
                .data("form", form(new SearchParams()));
    }

    /** Validates the form and answers with the streaming results area, or an inline error. */
    @GET
    @Path("results")
    public TemplateInstance results(@PathParam("name") String name, @BeanParam SearchParams params) {
        SearchForm form = form(params);
        try {
            form.toRequest(name);
            return Templates.results(name, form, null);
        } catch (UserErrorException e) {
            return Templates.results(name, form, e.error().message());
        }
    }

    /** Server-sent events {@code match} (row HTML), {@code progress} and {@code done}. */
    @GET
    @Path("search")
    @Produces(MediaType.SERVER_SENT_EVENTS)
    public Multi<OutboundSseEvent> stream(@PathParam("name") String name, @BeanParam SearchParams params,
            @Context Sse sse) {
        Credentials creds = credentials.current();
        SearchRequest request;
        try {
            request = form(params).toRequest(name);
        } catch (UserErrorException e) {
            return Multi.createFrom().item(event(sse, "done", Templates.failed(e.error().message())));
        }
        return search.search(creds, request)
                .map(e -> switch (e) {
                    case SearchEvent.Match m -> event(sse, "match", Templates.row(name, m.message()));
                    case SearchEvent.Progress p -> event(sse, "progress", Templates.progress(p));
                    case SearchEvent.Done d -> event(sse, "done", Templates.done(d));
                })
                .onFailure().recoverWithItem(t -> event(sse, "done", Templates.failed(failure(name, t))));
    }

    @GET
    @Path("{partition: \\d+}/{offset: \\d+}")
    public TemplateInstance detail(@PathParam("name") String name, @PathParam("partition") int partition,
            @PathParam("offset") long offset, @QueryParam("keyAs") String keyAs, @QueryParam("valueAs") String valueAs) {
        Format keyFormat = format(keyAs);
        Format valueFormat = format(valueAs);
        MessageView m = search.fetch(credentials.current(), name, partition, offset, keyFormat, valueFormat)
                .orElseThrow(() -> new UserErrorException(new UserError("Not found",
                        "Message at partition " + partition + ", offset " + offset + " of topic " + name
                                + " does not exist.", 404)));
        return Templates.detail(name, m, keyFormat == null ? "" : keyFormat.name(),
                valueFormat == null ? "" : valueFormat.name(), DECODE_AS);
    }

    private SearchForm form(SearchParams p) {
        return SearchForm.of(p.partitions, p.from, p.to, p.byOffset, p.offset, p.keyRegex, p.valueRegex,
                config.search().defaultRange(), Instant.now());
    }

    /** Unknown or empty means automatic. */
    private static Format format(String name) {
        return Arrays.stream(Format.values())
                .filter(f -> DECODE_AS.contains(f) && f.name().equalsIgnoreCase(name))
                .findFirst().orElse(null);
    }

    private static String failure(String topic, Throwable t) {
        if (t instanceof UserErrorException e) {
            return e.error().message();
        }
        UserError error = KafkaErrors.translate(t, "topic " + topic);
        if (error.httpStatus() == 500) {
            LOG.error("Message search failed", t);
        } else {
            LOG.warnf("Message search failed: %s (%s)", error.message(), t.toString());
        }
        return error.message();
    }

    private static OutboundSseEvent event(Sse sse, String name, TemplateInstance html) {
        return sse.newEventBuilder().name(name).data(String.class, html.render()).build();
    }
}
