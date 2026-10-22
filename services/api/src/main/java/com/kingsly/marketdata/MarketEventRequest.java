package com.kingsly.marketdata;

import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.PositiveOrZero;
import jakarta.validation.constraints.Min;
import java.math.BigDecimal;
import java.time.Instant;

public record MarketEventRequest(
        @NotBlank String eventId,
        @NotBlank String idempotencyKey,
        @Min(1) int schemaVersion,
        @NotBlank String producer,
        @NotBlank @Pattern(regexp = "[A-Za-z.]{1,12}") String symbol,
        @NotBlank @Pattern(regexp = "QUOTE|TRADE") String eventType,
        @NotNull @DecimalMin(value = "0.0", inclusive = false) BigDecimal price,
        @NotNull @PositiveOrZero Long quantity,
        @NotBlank String currency,
        @NotBlank String source,
        @NotNull Instant eventTime,
        @NotNull Instant ingestedAt,
        String newsText) {
}
