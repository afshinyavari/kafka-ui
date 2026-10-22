package se.afshin.yavari.kafka.ui.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.admin.Admin;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import se.afshin.yavari.kafka.ui.config.KafkaUiConfig;
import se.afshin.yavari.kafka.ui.security.Credentials;
import se.afshin.yavari.kafka.ui.security.Mode;
import se.afshin.yavari.kafka.ui.security.TestCerts;
import se.afshin.yavari.kafka.ui.security.TlsMaterial;
import se.afshin.yavari.kafka.ui.security.TlsMaterialHolder;

class KafkaClientsTest {

    @TempDir Path dir;

    /** Holder with a controllable generation; never touches the file system watcher. */
    static class FakeHolder extends TlsMaterialHolder {
        final TlsMaterial material;
        final AtomicLong generation = new AtomicLong();
        final List<Runnable> listeners = new ArrayList<>();

        FakeHolder(KafkaUiConfig cfg, TlsMaterial material) {
            super(cfg);
            this.material = material;
        }

        @Override public TlsMaterial current() { return material; }
        @Override public long generation() { return generation.get(); }
        @Override public void addListener(Runnable r) { listeners.add(r); }
        void reload() { generation.incrementAndGet(); listeners.forEach(Runnable::run); }
    }

    static class TestClients extends KafkaClients {
        final List<Admin> created = new CopyOnWriteArrayList<>();
        final List<Admin> closed = new CopyOnWriteArrayList<>();

        TestClients(KafkaUiConfig cfg, TlsMaterialHolder holder) {
            super(cfg, holder, p -> p, Runnable::run);
        }

        @Override
        protected Admin createAdmin(Credentials c) {
            Admin[] self = new Admin[1];
            self[0] = (Admin) Proxy.newProxyInstance(Admin.class.getClassLoader(), new Class<?>[] {Admin.class},
                    (proxy, method, args) -> {
                        if (method.getName().equals("close")) {
                            closed.add(self[0]);
                        }
                        return switch (method.getName()) {
                            case "equals" -> proxy == args[0];
                            case "hashCode" -> System.identityHashCode(proxy);
                            case "toString" -> "admin-proxy";
                            default -> null;
                        };
                    });
            created.add(self[0]);
            return self[0];
        }
    }

    FakeHolder holder;
    TestClients clients;
    final Instant later = Instant.now().plusSeconds(600);

    @BeforeEach
    void setUp() {
        var g = TestCerts.generate(dir);
        var cfg = KafkaClientPropertiesTest.cfg(Mode.OIDC);
        KafkaUiConfig withTls = new KafkaUiConfig() {
            public Mode mode() { return Mode.OIDC; }
            public String bootstrapServers() { return "b:1"; }
            public Tls tls() {
                return new Tls() {
                    public Optional<Path> certFile() { return Optional.of(g.certFile()); }
                    public Optional<Path> keyFile() { return Optional.of(g.keyFile()); }
                    public Path caFile() { return g.caFile(); }
                };
            }
            public Registry registry() { throw new UnsupportedOperationException(); }
            public Search search() { throw new UnsupportedOperationException(); }
            public Tail tail() { throw new UnsupportedOperationException(); }
            public Produce produce() { throw new UnsupportedOperationException(); }
        };
        TlsMaterial m = TlsMaterial.load(Optional.of(g.certFile()), Optional.of(g.keyFile()), g.caFile());
        holder = new FakeHolder(withTls, m);
        clients = new TestClients(cfg, holder);
    }

    @Test
    void adminIsCachedPerCacheScopeAndTokenAndRecreatedOnNewToken() {
        var alice1 = new Credentials.Bearer("t1", "alice", later);
        Admin a = clients.admin(alice1);
        assertThat(clients.admin(new Credentials.Bearer("t1", "alice", later))).isSameAs(a);
        assertThat(clients.admin(new Credentials.Bearer("t1", "bob", later))).isNotSameAs(a);
        Admin renewed = clients.admin(new Credentials.Bearer("t2", "alice", later));
        assertThat(renewed).isNotSameAs(a);
        assertThat(clients.created).hasSize(3);
    }

    @Test
    void expiredTokenEntryIsNotReused() {
        var expired = new Credentials.Bearer("t1", "alice", Instant.now().minusSeconds(1));
        Admin a = clients.admin(expired);
        assertThat(clients.admin(expired)).isNotSameAs(a);
        assertThat(clients.closed).contains(a);
    }

    @Test
    void entryExpiringWithinSkewIsNotReused() {
        var soon = new Credentials.Bearer("t1", "alice", Instant.now().plusSeconds(20));
        Admin a = clients.admin(soon);
        assertThat(clients.admin(soon)).isNotSameAs(a);
    }

    @Test
    void adminCacheEvictsOnTlsReload() {
        var c = new Credentials.Bearer("t1", "alice", later);
        Admin a = clients.admin(c);
        holder.reload();
        assertThat(clients.closed).containsExactly(a);
        assertThat(clients.admin(c)).isNotSameAs(a);
    }
}
