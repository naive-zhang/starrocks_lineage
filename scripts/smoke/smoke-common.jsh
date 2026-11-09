// Shared helpers of the smoke jshell scripts (loaded with /open). Configuration comes from environment variables:
//   SR_FE_HOST (required), SR_QUERY_PORT=9030, SR_HTTP_PORT=8030, SR_USER=lineage_etl, SR_PASSWORD
//   SR_BE_REWRITE="be-host:8040=reachable-host:port" when stream load redirects to a BE address that is not
//   reachable from this machine (NAT, Kubernetes, containers)
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import com.starrocks.lineage.analyzer.StarRocksSqlAnalyzer;
import com.starrocks.lineage.metadata.JdbcStarRocksMetadataProvider;
import com.starrocks.lineage.model.*;
import com.starrocks.lineage.pipeline.*;
import java.net.URI;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.UnaryOperator;

String env(String key, String def) {
    String v = System.getenv(key);
    return v == null || v.isEmpty() ? def : v;
}

HikariDataSource dataSource() {
    HikariConfig hc = new HikariConfig();
    hc.setJdbcUrl("jdbc:mysql://" + env("SR_FE_HOST", "127.0.0.1") + ":" + env("SR_QUERY_PORT", "9030")
            + "/?useSSL=false&allowPublicKeyRetrieval=true&connectTimeout=10000&socketTimeout=600000");
    hc.setUsername(env("SR_USER", "lineage_etl"));
    hc.setPassword(env("SR_PASSWORD", ""));
    hc.setMaximumPoolSize(8);
    return new HikariDataSource(hc);
}

UnaryOperator<URI> beRewrite() {
    String spec = env("SR_BE_REWRITE", "");
    if (spec.isEmpty()) {
        return UnaryOperator.identity();
    }
    String[] fromTo = spec.split("=");
    String[] from = fromTo[0].split(":");
    String[] to = fromTo[1].split(":");
    return uri -> {
        if (!uri.getHost().equals(from[0]) || uri.getPort() != Integer.parseInt(from[1])) {
            return uri;
        }
        try {
            return new URI(uri.getScheme(), uri.getUserInfo(), to[0], Integer.parseInt(to[1]), uri.getPath(),
                    uri.getQuery(), uri.getFragment());
        } catch (java.net.URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    };
}

StreamLoadClient streamLoad() {
    return new StreamLoadClient(URI.create("http://" + env("SR_FE_HOST", "127.0.0.1") + ":"
            + env("SR_HTTP_PORT", "8030")), env("SR_USER", "lineage_etl"), env("SR_PASSWORD", ""),
            Duration.ofMinutes(5), beRewrite());
}

/** The pipeline compares audit timestamps with the JVM clock: both must use the StarRocks time zone. */
void checkClock(javax.sql.DataSource ds) throws Exception {
    try (var c = ds.getConnection(); var st = c.createStatement(); var rs = st.executeQuery("SELECT NOW()")) {
        rs.next();
        LocalDateTime db = rs.getTimestamp(1).toLocalDateTime();
        LocalDateTime jvm = LocalDateTime.now();
        long skew = Math.abs(Duration.between(db, jvm).getSeconds());
        System.out.println("SMOKE_CLOCK starrocks=" + db + " jvm=" + jvm + " zone=" + ZoneId.systemDefault()
                + " skew_seconds=" + skew);
        if (skew > 120) {
            throw new IllegalStateException("JVM clock / time zone differs from StarRocks by " + skew
                    + "s: start jshell with -J-Duser.timezone=<StarRocks time_zone>");
        }
    }
}

String sqlTime(LocalDateTime t) {
    return t.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
}
