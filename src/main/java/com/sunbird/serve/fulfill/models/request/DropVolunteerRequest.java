package com.sunbird.serve.fulfill.models.request;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

@Data
public class DropVolunteerRequest {
    @NotBlank(message = "A reason for dropping the volunteer is required")
    private String comments;
}
