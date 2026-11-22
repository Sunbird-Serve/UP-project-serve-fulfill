package com.sunbird.serve.fulfill.models.request;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DropVolunteerResponse {
    private String nominationId;
    private String status;
    private int sessionsPaused;
}
