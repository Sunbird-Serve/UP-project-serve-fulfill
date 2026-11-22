package com.sunbird.serve.fulfill.VolunteeringService.services;

import com.sunbird.serve.fulfill.NominationMapper;
import com.sunbird.serve.fulfill.VolunteeringService.repositories.NominationRepository;
import com.sunbird.serve.fulfill.exception.ExternalServiceException;
import com.sunbird.serve.fulfill.exception.ServiceException;
import com.sunbird.serve.fulfill.exception.ApiException;
import com.sunbird.serve.fulfill.models.request.DropVolunteerResponse;
import com.sunbird.serve.fulfill.models.needresponse.NeedResponse;
import com.sunbird.serve.fulfill.models.userresponse.UserResponse;
import jakarta.mail.MessagingException;
import jakarta.mail.internet.MimeMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import com.sunbird.serve.fulfill.models.Nomination.Nomination;
import com.sunbird.serve.fulfill.models.Need.Need;
import com.sunbird.serve.fulfill.models.enums.NominationStatus;
import com.sunbird.serve.fulfill.models.enums.FulfillmentStatus;
import com.sunbird.serve.fulfill.models.enums.NeedStatus;
import com.sunbird.serve.fulfill.models.request.NominationRequest;
import com.sunbird.serve.fulfill.models.request.NeedPlanRequest;
import com.sunbird.serve.fulfill.models.request.NeedRequest;
import com.sunbird.serve.fulfill.models.request.UserStatusRequest;
import com.sunbird.serve.fulfill.models.Need.NeedPlan;
import com.sunbird.serve.fulfill.models.request.FulfillmentRequest;
import com.sunbird.serve.fulfill.models.request.NeedRequirementRequest;
import org.springframework.beans.factory.annotation.Value;

import java.util.*;

import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.client.*;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import org.springframework.http.MediaType;
import org.springframework.http.HttpHeaders;
import reactor.core.publisher.Mono;
import org.springframework.web.reactive.function.BodyInserters;

import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import java.util.concurrent.Future;
import java.util.concurrent.CompletableFuture;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Service
@Slf4j
public class NominationService {

    private final NominationRepository nominationRepository;
    private final WebClient webClient;
    private final NeedPlanRequest needPlanRequest;
    private final FulfillmentRequest fulfillmentRequest;
    private final FulfillmentService fulfillmentService;
    private final String serveUrl;
    private final String serveNeedUrl;
    private final String serveVolunteeringUserUrl;
    private final RestTemplate restTemplate;

    @Autowired
    private com.sunbird.serve.fulfill.FulfillmentRepository fulfillmentRepository;

    @Autowired
    private JavaMailSender javaMailSender;

    @Autowired
    private EmailTemplateService emailTemplateService;

    private static final Logger logger = LoggerFactory.getLogger(NominationService.class);

    /**
     * Extracts only the Authorization header for forwarding to downstream services.
     * Avoids forwarding host, origin, referer etc. which can confuse reverse proxies.
     */
    private void forwardAuthHeader(HttpHeaders httpHeaders, Map<String, String> headers) {
        String auth = headers.get("authorization");
        if (auth != null) {
            httpHeaders.set(HttpHeaders.AUTHORIZATION, auth);
        }
    }

    @Autowired
    public NominationService(NominationRepository nominationRepository,
                             WebClient.Builder webClientBuilder, NeedPlanRequest needPlanRequest,
                             FulfillmentRequest fulfillmentRequest,
                             FulfillmentService fulfillmentService, RestTemplate restTemplate, 
                             @Value("${serve.url}") String serveUrl,
                             @Value("${serve.need.url}") String serveNeedUrl,
                             @Value("${serve.volunteering.url}") String serveVolunteeringUserUrl) {
        this.nominationRepository = nominationRepository;
        this.webClient = webClientBuilder.baseUrl(serveUrl).build();
        this.needPlanRequest = needPlanRequest;
        this.fulfillmentRequest = fulfillmentRequest;
        this.fulfillmentService = fulfillmentService;
        this.restTemplate = restTemplate;
        this.serveUrl = serveUrl;
        this.serveNeedUrl = serveNeedUrl;
        this.serveVolunteeringUserUrl = serveVolunteeringUserUrl;
        this.restTemplate.getMessageConverters().add(new MappingJackson2HttpMessageConverter());
    }

    public Nomination nominateNeed(NominationRequest nominationRequest, Map<String, String> headers) {
        try {
            // Convert NominationRequest to Nomination entity
            Nomination nomination = NominationMapper.mapToEntity(nominationRequest);
            String status = nominationRequest.getStatus().toString();
            String apiNeedUrl = String.format("%s/api/v1/serve-need/need/status/%s?status=%s", serveNeedUrl, nomination.getNeedId(), status);
            webClient.put()
                    .uri(apiNeedUrl)
                    .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                    .headers(httpHeaders -> forwardAuthHeader(httpHeaders, headers))
                    .retrieve()
                    .toBodilessEntity()
                    .block();
            // Save the entity
            return nominationRepository.save(nomination);
        } catch (WebClientResponseException e) {
            logger.error("External service error while nominating need: {}", e.getMessage(), e);
            throw new ExternalServiceException(
                "Failed to update need status", 
                "NEED_SERVICE_ERROR", 
                "serve-need", 
                HttpStatus.valueOf(e.getStatusCode().value()), 
                serveNeedUrl + "/api/v1/serve-need/need/status/" + nominationRequest.getNeedId()
            );
        } catch (Exception e) {
            logger.error("Failed to nominate need: {}", e.getMessage(), e);
            throw new ServiceException("Failed to nominate need", "NOMINATION_CREATION_ERROR", "nomination-service", e);
        }
    }

    // Update nomination as confirm or reject
    public Nomination updateNomination(String userId, String nominationId, NominationStatus status, String comments, Map<String, String> headers) {
        Nomination nomination = nominationRepository.findById(UUID.fromString(nominationId)).get();
        //Need need = needRepository.findById(UUID.fromString(nomination.getNeedId())).get();
        UserStatusRequest userStatusRequest = new UserStatusRequest();
            nomination.setNominationStatus(status);
            if (comments != null && !comments.trim().isEmpty()) {
                nomination.setComments(comments);
            }
            List<Nomination> nominationList = getAllNominations(nomination.getNeedId(), headers);
        String needStatus = "";

            if (status.equals(NominationStatus.Approved)) {
        /*for (Nomination n : nominationList) {
            if (!n.getId().equals(nomination.getId())) {
                n.setNominationStatus(NominationStatus.Rejected);
                nominationRepository.save(n);
            }
        }*/
        needStatus = "Assigned";
        userStatusRequest.setStatus("Active");
        userStatusRequest.setSend(true);
        String apiUserUrl = String.format("%s/api/v1/serve-volunteering/user/status/update/%s",serveVolunteeringUserUrl, userId);
        webClient.put()
            .uri(apiUserUrl)
            .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
            .headers(httpHeaders -> forwardAuthHeader(httpHeaders, headers))
            .body(BodyInserters.fromValue(userStatusRequest))
            .retrieve()
            .bodyToMono(Void.class)
            .block();
    } else if (status.equals(NominationStatus.Rejected) || status.equals(NominationStatus.Backfill) ) {
        // Check if any other nomination is approved
        boolean anyApproved = nominationList.stream()
                .anyMatch(n -> n.getNominationStatus().equals(NominationStatus.Approved));
        boolean anyNominated = nominationList.stream()
                .anyMatch(n -> n.getNominationStatus().equals(NominationStatus.Nominated));

        if (anyApproved) {
            needStatus = "Assigned";
        } else if (anyNominated) {
            needStatus = "Nominated";
        } else {
            needStatus = "Approved";
        }
    }

        String apiNeedUrl = String.format("%s/api/v1/serve-need/need/status/%s?status=%s", serveNeedUrl, nomination.getNeedId(),needStatus);
        webClient.put()
                    .uri(apiNeedUrl)
                    .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                    .headers(httpHeaders -> forwardAuthHeader(httpHeaders, headers))
                    .retrieve()
                    .toBodilessEntity()
                    .block();

        if (status.equals(NominationStatus.Approved)){
            // Call the createNeedPlan API
            String needPlanId = callCreateNeedPlanApi(needPlanRequest,nomination.getNeedId(), headers);

            // Create Fulfillment Details for this need
            createFulfillmentDetails(fulfillmentRequest, nomination.getNeedId(), needPlanId, nomination.getNominatedUserId(), headers);
        }
        return nominationRepository.save(nomination);
    }

    /**
     * Drops an approved volunteer: pauses the sessions of their need plans via the need service,
     * then marks the nomination Backfill. Safe to retry when the nomination is already Backfill.
     */
    public DropVolunteerResponse dropVolunteer(String nominationId, String comments, Map<String, String> headers,
                                               org.springframework.security.core.Authentication authentication) {
        if (comments == null || comments.trim().isEmpty()) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "A reason for dropping the volunteer is required");
        }

        Nomination nomination;
        try {
            nomination = nominationRepository.findById(UUID.fromString(nominationId)).orElse(null);
        } catch (IllegalArgumentException e) {
            nomination = null;
        }
        if (nomination == null) {
            throw new ApiException(HttpStatus.NOT_FOUND, "Nomination not found");
        }

        // A caller without a staff role (i.e. a Volunteer) may only drop their own nomination.
        boolean droppedByVolunteer = !isStaffCaller(authentication);
        if (droppedByVolunteer && !isNominationOwner(authentication, nomination)) {
            throw new ApiException(HttpStatus.FORBIDDEN, "You can only drop your own nomination");
        }

        NominationStatus current = nomination.getNominationStatus();
        if (current != NominationStatus.Approved && current != NominationStatus.Backfill) {
            throw new ApiException(HttpStatus.CONFLICT,
                    "Only an approved volunteer can be dropped. Current nomination status: " + current);
        }

        List<String> needPlanIds = new ArrayList<>();
        for (com.sunbird.serve.fulfill.models.Nomination.Fulfillment f :
                fulfillmentRepository.findAllByNeedIdAndAssignedUserId(nomination.getNeedId(), nomination.getNominatedUserId())) {
            String planId = f.getNeedPlanId();
            if (planId != null && !planId.trim().isEmpty() && !needPlanIds.contains(planId)) {
                needPlanIds.add(planId);
            }
        }
        if (needPlanIds.isEmpty()) {
            throw new ApiException(HttpStatus.CONFLICT, "No need plan found for this volunteer and need");
        }

        int sessionsPaused = 0;
        Map<String, String> body = new HashMap<>();
        body.put("reason", comments.trim());
        for (String needPlanId : needPlanIds) {
            String url = serveNeedUrl + "/api/v1/serve-need/need-plan/" + needPlanId + "/drop-volunteer";
            try {
                Map<?, ?> result = webClient.put()
                        .uri(url)
                        .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                        .headers(httpHeaders -> forwardAuthHeader(httpHeaders, headers))
                        .bodyValue(body)
                        .retrieve()
                        .bodyToMono(Map.class)
                        .block();
                if (result != null && result.get("pausedSessions") instanceof Number) {
                    sessionsPaused += ((Number) result.get("pausedSessions")).intValue();
                }
            } catch (Exception e) {
                logger.error("Need service failed to pause plan {} for nomination {}: {}", needPlanId, nominationId, e.getMessage(), e);
                throw new ExternalServiceException(
                        "Could not pause the volunteer's sessions. The nomination was not changed. Please try again.",
                        "NEED_SERVICE_ERROR", "serve-need", HttpStatus.BAD_GATEWAY, url);
            }
        }

        // Last step, so a failure above leaves the Drop button available for a retry.
        nomination.setNominationStatus(NominationStatus.Backfill);
        nomination.setComments(comments.trim());
        nominationRepository.save(nomination);

        if (droppedByVolunteer) {
            notifyCoordinatorOfDrop(nomination.getNeedId(), nomination.getNominatedUserId(), comments.trim());
        }

        return new DropVolunteerResponse(nomination.getId().toString(), NominationStatus.Backfill.name(), sessionsPaused);
    }

    private static final Set<String> STAFF_AUTHORITIES = Set.of(
            "ROLE_sAdmin", "ROLE_nAdmin", "ROLE_nCoordinator", "ROLE_vAdmin", "ROLE_vCoordinator");

    private boolean isStaffCaller(org.springframework.security.core.Authentication authentication) {
        return authentication != null && authentication.getAuthorities().stream()
                .anyMatch(a -> STAFF_AUTHORITIES.contains(a.getAuthority()));
    }

    /** Owner means the token subject equals the nominated user id, or the token email matches that user's email. */
    private boolean isNominationOwner(org.springframework.security.core.Authentication authentication, Nomination nomination) {
        if (!(authentication instanceof org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken jwtAuth)) {
            return false;
        }
        org.springframework.security.oauth2.jwt.Jwt jwt = jwtAuth.getToken();
        String nominatedUserId = nomination.getNominatedUserId();
        if (nominatedUserId == null) {
            return false;
        }
        if (nominatedUserId.equals(jwt.getSubject())) {
            return true;
        }
        String tokenEmail = jwt.getClaimAsString("email");
        if (tokenEmail == null || tokenEmail.isBlank()) {
            return false;
        }
        try {
            UserResponse user = fetchUserResponse(nominatedUserId);
            String userEmail = user.getContactDetails().getEmail();
            return userEmail != null && userEmail.trim().equalsIgnoreCase(tokenEmail.trim());
        } catch (Exception e) {
            logger.warn("Could not verify nomination ownership for nomination user {}: {}", nominatedUserId, e.getMessage());
            return false;
        }
    }

    /** Fire-and-forget. Any failure is logged and never affects the drop. */
    private void notifyCoordinatorOfDrop(String needId, String nominatedUserId, String reason) {
        CompletableFuture.runAsync(() -> {
            try {
                NeedResponse need = fetchNeedResponse(needId);
                UserResponse coordinator = fetchUserResponse(need.getUserId());
                UserResponse volunteer = fetchUserResponse(nominatedUserId);

                MimeMessage mimeMessage = javaMailSender.createMimeMessage();
                MimeMessageHelper helper = new MimeMessageHelper(mimeMessage, true, "utf-8");
                helper.setTo(coordinator.getContactDetails().getEmail());
                helper.setSubject(emailTemplateService.getVolunteerDropEmailSubject());
                helper.setText(emailTemplateService.getVolunteerDropEmailBody(
                        coordinator.getIdentityDetails().getFullname(),
                        volunteer.getIdentityDetails().getFullname(),
                        need.getDescription(),
                        reason), true);
                javaMailSender.send(mimeMessage);
            } catch (Exception e) {
                logger.error("Failed to email coordinator about volunteer drop for needId {}", needId, e);
            }
        });
    }

    private String callCreateNeedPlanApi(NeedPlanRequest request, String needId,
    Map<String, String> headers) {
        try{
            String apiNeedUrl = serveNeedUrl+"/api/v1/serve-need/need/"+needId;
            String apiUrl = serveNeedUrl+"/api/v1/serve-need/need-plan/create";
            String apiNeedReqUrl = serveNeedUrl+"/api/v1/serve-need/need-requirement/";

            // Get Need details
            NeedRequest needRequest = webClient.get()
                    .uri(apiNeedUrl, needId)
                    .headers(httpHeaders -> forwardAuthHeader(httpHeaders, headers))
                    .retrieve()
                    .bodyToMono(NeedRequest.class)
                    .block();

            apiNeedReqUrl = apiNeedReqUrl+needRequest.getRequirementId();
            // Get Need Requirement details
            NeedRequirementRequest needRequirementRequest = webClient.get()
                    .uri(apiNeedReqUrl, needRequest.getRequirementId())
                    .headers(httpHeaders -> forwardAuthHeader(httpHeaders, headers))
                    .retrieve()
                    .bodyToMono(NeedRequirementRequest.class)
                    .block();


            request.setNeedId(needId);
            request.setName(needRequest.getName());
            request.setStatus(NeedStatus.Approved);
            request.setOccurrenceId(needRequirementRequest.getOccurrenceId());

            ResponseEntity<NeedPlan> responseEntity = webClient.post()
                    .uri(apiUrl)
                    .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON_VALUE)
                    .headers(httpHeaders -> forwardAuthHeader(httpHeaders, headers))
                    .body(Mono.just(request), NeedPlanRequest.class)
                    .exchangeToMono(response -> response.toEntity(NeedPlan.class))
                    .block();

            // Extract the newly created Need Plan ID from the response
            String needPlanId = null;
            if (responseEntity != null && responseEntity.getBody() != null) {
                needPlanId = responseEntity.getBody().getId().toString();
            }

            return needPlanId;
        }catch (WebClientResponseException e) {
            logger.error("External service error while creating need plan: {}", e.getMessage(), e);
            throw new ExternalServiceException(
                "Failed to create need plan", 
                "NEED_SERVICE_ERROR", 
                "serve-need", 
                HttpStatus.valueOf(e.getStatusCode().value()), 
                serveNeedUrl + "/api/v1/serve-need/need-plan/create"
            );
        } catch (Exception e) {
            logger.error("Failed to get need plan created from fulfill ms: {}", e.getMessage(), e);
            throw new ServiceException("Failed to create need plan from fulfill ms", "NEED_PLAN_CREATION_ERROR", "nomination-service", e);
        }
    }


     private void createFulfillmentDetails(FulfillmentRequest request, String needId,
     String needPlanId,
     String assignedUserId,
     Map<String, String> headers) {
        try{
            String apiNeedUrl = serveNeedUrl+"/api/v1/serve-need/need/"+needId;
            // Get Need details
            NeedRequest needRequest = webClient.get()
                    .uri(apiNeedUrl, needId)
                    .headers(httpHeaders -> forwardAuthHeader(httpHeaders, headers))
                    .retrieve()
                    .bodyToMono(NeedRequest.class)
                    .block();

            request.setNeedId(needId);
            request.setNeedPlanId(needPlanId);
            request.setAssignedUserId(assignedUserId);
            request.setCoordUserId(needRequest.getUserId());
            request.setFulfillmentStatus(FulfillmentStatus.InProgress);
            fulfillmentService.createFulfillment(request);
        }catch (WebClientResponseException e) {
            logger.error("External service error while creating fulfillment details: {}", e.getMessage(), e);
            throw new ExternalServiceException(
                "Failed to fetch need details for fulfillment", 
                "NEED_SERVICE_ERROR", 
                "serve-need", 
                HttpStatus.valueOf(e.getStatusCode().value()), 
                serveNeedUrl + "/api/v1/serve-need/need/" + needId
            );
        } catch (Exception e) {
            logger.error("Failed to create fulfillment: {}", e.getMessage(), e);
            throw new ServiceException("Failed to create fulfillment", "FULFILLMENT_CREATION_ERROR", "nomination-service", e);
        }
     }

    public List<Nomination> getAllNominations(String needId, Map<String, String> headers) {
        return nominationRepository.findAllByNeedId(needId);
    }

    public List<Nomination> getAllNominationsByStatus(String needId, NominationStatus status, Map<String, String> headers) {
        return nominationRepository.findAllByNeedIdAndNominationStatus(needId, status);
    }


    public List<Nomination> getAllNominationForUser(String nominatedUserId, Integer page, Integer size, Map<String, String> headers) {
        return nominationRepository.findAllByNominatedUserId(nominatedUserId);
    }

    public void fetchNCoordinatorEmail(String needId, String nominatedUserId) {
    CompletableFuture<NeedResponse> needResponseFuture = fetchNeedResponseAsync(needId);
    CompletableFuture<UserResponse> nCoordinatorResponseFuture = needResponseFuture.thenCompose(needResponse ->
            fetchUserResponseAsync(needResponse.getUserId()));
    CompletableFuture<UserResponse> nominatedUserResponseFuture = fetchUserResponseAsync(nominatedUserId);

    CompletableFuture.allOf(needResponseFuture, nCoordinatorResponseFuture, nominatedUserResponseFuture)
            .thenRun(() -> {
                try {
                    NeedResponse needResponse = needResponseFuture.get();
                    UserResponse nCoordinatorResponse = nCoordinatorResponseFuture.get();
                    UserResponse nominatedUserResponse = nominatedUserResponseFuture.get();

                    sendEmailToNCoordinatorAsync(
                            nCoordinatorResponse.getIdentityDetails().getFullname(),
                            nCoordinatorResponse.getContactDetails().getEmail(),
                            needResponse.getDescription(),
                            nominatedUserResponse.getIdentityDetails().getFullname()
                    );
                } catch (Exception e) {
                    logger.error("Failed to send email to coordinator for needId: {} and nominatedUserId: {}", needId, nominatedUserId, e);
                }
            });
}

    public void fetchNominatedUserEmail(String nominatedUserId) {
    fetchUserResponseAsync(nominatedUserId).thenAccept(userResponse -> {
        sendEmailToNominatedUserAsync(
                userResponse.getIdentityDetails().getFullname(),
                userResponse.getContactDetails().getEmail()
        );
    });
}

    public CompletableFuture<NeedResponse> fetchNeedResponseAsync(String needId) {
        return CompletableFuture.supplyAsync(() -> fetchNeedResponse(needId));
    }

    public CompletableFuture<UserResponse> fetchUserResponseAsync(String userId) {
        return CompletableFuture.supplyAsync(() -> fetchUserResponse(userId));
    }

    private NeedResponse fetchNeedResponse(String needId) {
        String serveNeedApi = serveNeedUrl+"/api/v1/serve-need/need/" + needId;
        ResponseEntity<NeedResponse> responseEntity = restTemplate.getForEntity(serveNeedApi, NeedResponse.class);

        if (responseEntity.getStatusCode() != HttpStatus.OK) {
            throw new HttpStatusCodeException(responseEntity.getStatusCode(), "Failed to fetch need response") {};
        }
        return responseEntity.getBody();
    }

    private UserResponse fetchUserResponse(String userId) {
        String serveVolunteeringApi = serveVolunteeringUserUrl+"/api/v1/serve-volunteering/user/" + userId;
        ResponseEntity<UserResponse> responseEntity = restTemplate.getForEntity(serveVolunteeringApi, UserResponse.class);

        if (responseEntity.getStatusCode() != HttpStatus.OK) {
            throw new HttpStatusCodeException(responseEntity.getStatusCode(), "Failed to fetch user response") {};
        }
        return responseEntity.getBody();
    }

    @Async
    public CompletableFuture<Void> sendEmailToNCoordinatorAsync(String nCoordinatorName, String ncoordinatorEmail, String description, String nominatedUserName){
        try {
            sendEmailToNCoordinator(nCoordinatorName, ncoordinatorEmail, description, nominatedUserName);
        } catch (Exception e) {
            logger.error("Failed to send email to coordinator: {}", ncoordinatorEmail, e);
        }
        return CompletableFuture.completedFuture(null);
    }

    public void sendEmailToNCoordinator(String nCoordinatorName, String ncoordinatorEmail, String description, String nominatedUserName) {
        try {
            MimeMessage mimeMessage = javaMailSender.createMimeMessage();
            MimeMessageHelper mimeMessageHelper = new MimeMessageHelper(mimeMessage, true, "utf-8");
            String subject = emailTemplateService.getNCoordinatorEmailSubject();
            String body = emailTemplateService.getNCoordinatorEmailBody(nCoordinatorName, nominatedUserName, description);
            mimeMessageHelper.setTo(ncoordinatorEmail);
            mimeMessageHelper.setSubject(subject);
            mimeMessageHelper.setText(body, true);
            javaMailSender.send(mimeMessage);
        } catch (MessagingException e) {
            logger.error("Failed to send email to coordinator: {}", ncoordinatorEmail, e);
        }
    }


    @Async
    public CompletableFuture<Void> sendEmailToNominatedUserAsync(String nominatedUserName, String nominatedUserEmail) {
        try {
            sendEmailToNominatedUser(nominatedUserName, nominatedUserEmail);
        } catch (Exception e) {
            logger.error("Failed to send email to nominated user: {}", nominatedUserEmail, e);
        }
        return CompletableFuture.completedFuture(null);
    }

    public void sendEmailToNominatedUser(String nominatedUserName, String nominatedUserEmail) {
        try {
            MimeMessage mimeMessage = javaMailSender.createMimeMessage();
            MimeMessageHelper mimeMessageHelper = new MimeMessageHelper(mimeMessage, true, "utf-8");
            String subject = emailTemplateService.getNominatedUserEmailSubject();
            String body = emailTemplateService.getNominatedUserEmailBody(nominatedUserName);
            mimeMessageHelper.setTo(nominatedUserEmail);
            mimeMessageHelper.setSubject(subject);
            mimeMessageHelper.setText(body, true);
            javaMailSender.send(mimeMessage);
        } catch (MessagingException e) {
            logger.error("Failed to send email to nominated user: {}", nominatedUserEmail, e);
        }
    }



    @Async
    public CompletableFuture<Void> sendEmailToVolunteerAsync(String nominatedUserId, NominationStatus status, String needId) {
        try {
            sendEmailToVolunteer(nominatedUserId, status, needId);
        } catch (Exception e) {
            logger.error("Failed to send email to volunteer: {} for needId: {}", nominatedUserId, needId, e);
        }
        return CompletableFuture.completedFuture(null);
    }

    public void sendEmailToVolunteer(String nominatedUserId, NominationStatus status, String needId) {
        try {
            MimeMessage mimeMessage = javaMailSender.createMimeMessage();
            UserResponse userResponse = fetchUserResponse(nominatedUserId);
            String nominatedUserEmail = userResponse.getContactDetails().getEmail();
            String nominatedUserName = userResponse.getIdentityDetails().getFullname();
            NeedResponse needResponse = fetchNeedResponse(needId);
            String description = needResponse.getDescription();
            
            // Fetch coordinator information
            UserResponse coordinatorResponse = fetchUserResponse(needResponse.getUserId());
            String coordUserName = coordinatorResponse.getIdentityDetails().getFullname();
            String coordPhoneNumber = coordinatorResponse.getContactDetails().getMobile();
            
            // For entity name, we'll use the need name as a fallback since entity name might not be directly available
            String entityName = needResponse.getName() != null ? needResponse.getName() : "N/A";
            
            MimeMessageHelper mimeMessageHelper = new MimeMessageHelper(mimeMessage, true, "utf-8");
            String subject = emailTemplateService.getVolunteerEmailSubject(status);
            String body = emailTemplateService.getVolunteerEmailBody(nominatedUserName, status, description, entityName, coordUserName, coordPhoneNumber);

            mimeMessageHelper.setTo(nominatedUserEmail);
            mimeMessageHelper.setSubject(subject);
            mimeMessageHelper.setText(body, true);
            javaMailSender.send(mimeMessage);
        } catch (MessagingException e) {
            logger.error("Failed to send email to volunteer: {} for needId: {}", nominatedUserId, needId, e);
        }
    }


    public List<UserResponse> getRecommendedVolunteersNotNominated(Map<String, String> headers) {
        UserResponse[] allVolunteers = webClient.get()
                .uri("/api/v1/serve-volunteering/user/status?status=Recommended")
                .headers(httpHeaders -> forwardAuthHeader(httpHeaders, headers))
                .retrieve()
                .bodyToMono(UserResponse[].class)
                .block();

        if (allVolunteers == null) {
            log.warn("API returned null response body");
            return new ArrayList<>();
        }

        List<UserResponse> notNominatedVolunteers = new ArrayList<>();

        for (UserResponse user : allVolunteers) {
            if (user.getRole() != null && user.getRole().contains("Volunteer")) {
                List<Nomination> nominations = nominationRepository.findAllByNominatedUserId(user.getOsid());
                if (nominations == null || nominations.isEmpty()) {
                    notNominatedVolunteers.add(user);
                }
            }
        }

        if (notNominatedVolunteers.isEmpty()) {
            log.info("All users from the API are already nominated.");
        }

        return notNominatedVolunteers;
    }


    public List<UserResponse> getRecommendedVolunteersNominated(Map<String, String> headers) {
        UserResponse[] allVolunteers = webClient.get()
                .uri("/api/v1/serve-volunteering/user/status?status=Recommended")
                .headers(httpHeaders -> forwardAuthHeader(httpHeaders, headers))
                .retrieve()
                .bodyToMono(UserResponse[].class)
                .block();

        if (allVolunteers == null) {
            log.warn("API returned null response body");
            return new ArrayList<>();
        }

        List<UserResponse> nominatedVolunteers = new ArrayList<>();

        for (UserResponse user : allVolunteers) {
            if (user.getRole() != null && user.getRole().contains("Volunteer")) {
                List<Nomination> nominations = nominationRepository.findAllByNominatedUserId(user.getOsid());
                if (nominations != null && !nominations.isEmpty()) {
                    nominatedVolunteers.add(user);
                }
            }
        }

        if (nominatedVolunteers.isEmpty()) {
            log.info("No recommended volunteers are present in the nomination table.");
        }

        return nominatedVolunteers;
    }

}
