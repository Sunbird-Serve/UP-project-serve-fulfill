package com.sunbird.serve.fulfill;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import com.sunbird.serve.fulfill.models.Nomination.Nomination;
import com.sunbird.serve.fulfill.models.enums.NominationStatus;
import com.sunbird.serve.fulfill.models.request.NominationRequest;
import java.util.List;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.web.bind.annotation.RequestParam;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import org.springframework.http.MediaType;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.UUID;
import java.util.Map;


@RestController
@CrossOrigin(origins = "*")
public class NominationController {

    private final NominationService nominationService;

    @Autowired
    public NominationController(NominationService nominationService) {
        this.nominationService = nominationService;
    }

    //Nominate a need
    @Operation(summary = "User Nominate a Need", description = "Nominate a Need")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully Nominated a Need", content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE)),
            @ApiResponse(responseCode = "400", description = "Bad Input"),
            @ApiResponse(responseCode = "500", description = "Server Error")}
    )
    @PostMapping("/serve-fulfill/nomination/{needId}/nominate/{nominatedUserId}")
    public ResponseEntity<Nomination> nominateNeed( @PathVariable String needId,
            @PathVariable String nominatedUserId,
            @RequestHeader Map<String, String> headers) {

        NominationRequest request = new NominationRequest();
        request.setNeedId(needId);
        request.setNominatedUserId(nominatedUserId);
        request.setComments("");
        request.setStatus(NominationStatus.Nominated);
        Nomination response = nominationService.nominateNeed(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }


    //Confirm or reject the nomination
    @Operation(summary = "Confirm/Reject the nomination", description = "Confirm/Reject the nomination")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully Nominated a Need", content = @Content(mediaType = MediaType.APPLICATION_JSON_VALUE)),
            @ApiResponse(responseCode = "400", description = "Bad Input"),
            @ApiResponse(responseCode = "500", description = "Server Error")}
    )
    @PostMapping("/serve-fulfill/nomination/{needId}/nominate/{nominatedUserId}/confirm/{nominationId}")
    public ResponseEntity<Nomination> updateNomination( @PathVariable String nominatedUserId,
            @PathVariable String nominationId,
            @RequestParam(required = true) NominationStatus status,
            @RequestHeader Map<String, String> headers) {

        Nomination nominations = nominationService.updateNomination(nominatedUserId,nominationId,status, headers);
        return ResponseEntity.ok(nominations);
    }

    @GetMapping("/serve-fulfill/nomination/{needId}/nominate")
    public ResponseEntity<List<Nomination>> getAllNominations(@PathVariable String needId, @RequestHeader Map<String, String> headers) {
        List<Nomination> nominations = nominationService.getAllNominations(needId, headers);
        return ResponseEntity.ok(nominations);
    }

    @GetMapping("/serve-fulfill/nomination/{needId}/nominate/{status}")
    public ResponseEntity<List<Nomination>> getAllNominationsByStatus(
            @PathVariable String needId,
            @PathVariable NominationStatus status,
            @RequestHeader Map<String, String> headers) {
        List<Nomination> nominations = nominationService.getAllNominationsByStatus(needId, status, headers);
        return ResponseEntity.ok(nominations);
    }

    @GetMapping("/serve-fulfill/nomination/{nominatedUserId}")
    public ResponseEntity<List<Nomination>> getAllNominationForUser(
            @PathVariable String nominatedUserId,
            @RequestParam(defaultValue = "0") Integer page,
            @RequestParam(defaultValue = "10") Integer size,
            @RequestHeader Map<String, String> headers) {
        List<Nomination> nominations = nominationService.getAllNominationForUser(nominatedUserId, page, size, headers);
        return ResponseEntity.ok(nominations);
    }
    // Add other controller methods as needed
}
