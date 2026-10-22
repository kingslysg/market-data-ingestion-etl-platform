package com.kingsly.marketdata;

import java.util.Map;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
public class MarketController {
    private final JdbcTemplate jdbc;
    public MarketController(JdbcTemplate jdbc) { this.jdbc = jdbc; }

    @GetMapping("/health")
    public Map<String, Object> health() {
        try { jdbc.queryForObject("select 1", Integer.class); return Map.of("status", "UP", "database", "UP", "mode", "LOCAL_DEMO"); }
        catch (DataAccessException ex) { return Map.of("status", "DEGRADED", "database", "DOWN", "mode", "LOCAL_DEMO"); }
    }

    @GetMapping("/market/summary")
    public Map<String, Object> summary() {
        return jdbc.queryForMap("select count(*) as events, count(distinct idempotency_key) as unique_keys, count(*) filter (where event_type='TRADE') as trades, max(ingested_at) as last_ingest from market_event");
    }

    @GetMapping("/market/events")
    public java.util.List<Map<String, Object>> events(@RequestParam(defaultValue="50") int limit) {
        int bounded = Math.max(1, Math.min(limit, 500));
        return jdbc.queryForList("select event_id, symbol, event_type, price, quantity, source, event_time, ingested_at from market_event order by event_time desc limit ?", bounded);
    }
}
