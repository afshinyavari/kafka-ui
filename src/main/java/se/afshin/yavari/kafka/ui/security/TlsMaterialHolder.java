package se.afshin.yavari.kafka.ui.security;

import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import org.jboss.logging.Logger;
import se.afshin.yavari.kafka.ui.config.KafkaUiConfig;

/** Holds the current TlsMaterial, loaded lazily and reloaded when the PEM files change. */
@ApplicationScoped
public class TlsMaterialHolder implements AutoCloseable {

    private static final Logger LOG = Logger.getLogger(TlsMaterialHolder.class);
    private static final long DEBOUNCE_MS = 200;

    private final Optional<Path> cert;
    private final Optional<Path> key;
    private final Path ca;
    private final List<Runnable> listeners = new CopyOnWriteArrayList<>();

    private volatile TlsMaterial current;
    private volatile String fingerprint;
    private volatile long generation;
    private WatchService watchService;
    private Thread watcher;

    @Inject
    public TlsMaterialHolder(KafkaUiConfig config) {
        this(config.tls().certFile(), config.tls().keyFile(), config.tls().caFile());
    }

    TlsMaterialHolder(Optional<Path> cert, Optional<Path> key, Path ca) {
        this.cert = cert;
        this.key = key;
        this.ca = ca;
    }

    public TlsMaterial current() {
        TlsMaterial m = current;
        if (m != null) {
            return m;
        }
        synchronized (this) {
            if (current == null) {
                String fp = fingerprint();
                current = TlsMaterial.load(cert, key, ca);
                fingerprint = fp;
                startWatcher();
            }
            return current;
        }
    }

    public long generation() {
        return generation;
    }

    public void addListener(Runnable onReload) {
        listeners.add(onReload);
    }

    private List<Path> files() {
        List<Path> files = new ArrayList<>();
        cert.ifPresent(files::add);
        key.ifPresent(files::add);
        files.add(ca);
        return files;
    }

    private String fingerprint() {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            for (Path p : files()) {
                md.update(p.toString().getBytes());
                md.update(Files.readAllBytes(p));
            }
            return java.util.HexFormat.of().formatHex(md.digest());
        } catch (IOException | java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException("Cannot read TLS files " + files() + ": " + e.getMessage(), e);
        }
    }

    private void startWatcher() {
        try {
            watchService = FileSystems.getDefault().newWatchService();
            Set<Path> dirs = new LinkedHashSet<>();
            for (Path p : files()) {
                dirs.add(p.toAbsolutePath().getParent());
            }
            for (Path d : dirs) {
                d.register(
                        watchService,
                        StandardWatchEventKinds.ENTRY_CREATE,
                        StandardWatchEventKinds.ENTRY_MODIFY,
                        StandardWatchEventKinds.ENTRY_DELETE);
            }
        } catch (IOException e) {
            LOG.warnf(e, "Cannot watch TLS files %s for changes; reload disabled", files());
            return;
        }
        WatchService ws = watchService;
        watcher = new Thread(() -> watchLoop(ws), "tls-material-watcher");
        watcher.setDaemon(true);
        watcher.start();
    }

    private void watchLoop(WatchService ws) {
        try {
            while (true) {
                WatchKey k = ws.take();
                k.pollEvents();
                k.reset();
                // debounce: wait for the burst of events (kubernetes symlink swap, multi-file writes) to settle
                Thread.sleep(DEBOUNCE_MS);
                WatchKey more;
                while ((more = ws.poll()) != null) {
                    more.pollEvents();
                    more.reset();
                }
                reloadIfChanged();
            }
        } catch (InterruptedException | ClosedWatchServiceException e) {
            // shutting down
        }
    }

    private synchronized void reloadIfChanged() {
        try {
            String fp = fingerprint();
            if (fp.equals(fingerprint)) {
                return;
            }
            TlsMaterial loaded = TlsMaterial.load(cert, key, ca);
            current = loaded;
            fingerprint = fp;
            generation++;
            LOG.infof("TLS material reloaded (generation %d)", generation);
        } catch (RuntimeException e) {
            LOG.warnf("TLS reload failed, keeping previous material: %s", e.getMessage());
            return;
        }
        for (Runnable l : listeners) {
            try {
                l.run();
            } catch (RuntimeException e) {
                LOG.warn("TLS reload listener failed", e);
            }
        }
    }

    @PreDestroy
    @Override
    public void close() {
        try {
            if (watchService != null) {
                watchService.close();
            }
        } catch (IOException ignored) {
            // nothing to do
        }
        if (watcher != null) {
            watcher.interrupt();
        }
    }
}
