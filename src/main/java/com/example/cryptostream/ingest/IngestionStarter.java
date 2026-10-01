package com.example.cryptostream.ingest;

import com.example.cryptostream.config.AppProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

/**
 * Kicks off ingestion at startup when {@code crypto.auto-start=true}. Otherwise the feed
 * stays idle until POST /api/ingest/start.
 */
@Component
public class IngestionStarter implements ApplicationRunner {

    private static final Logger log = LoggerFactory.getLogger(IngestionStarter.class);

    private final AppProperties props;
    private final IngestionService ingestion;

    public IngestionStarter(AppProperties props, IngestionService ingestion) {
        this.props = props;
        this.ingestion = ingestion;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (props.autoStart()) {
            log.info("crypto.auto-start=true — starting ingestion");
            ingestion.start();
        } else {
            log.info("crypto.auto-start=false — POST /api/ingest/start to begin");
        }
    }
}
