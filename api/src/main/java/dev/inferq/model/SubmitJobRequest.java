package dev.inferq.model;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record SubmitJobRequest(
        @NotBlank @Size(max = 32_000) String prompt,
        String model,
        Priority priority,
        @Min(1) @Max(4096) Integer maxTokens,
        @Min(1) @Max(10) Integer maxAttempts) {}
