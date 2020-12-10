package com.devshawn.kafka.gitops.domain.state.service;

import com.devshawn.kafka.gitops.domain.options.GetAclOptions;
import com.devshawn.kafka.gitops.domain.state.AclDetails;
import com.devshawn.kafka.gitops.domain.state.ServiceDetails;
import com.devshawn.kafka.gitops.exception.InvalidAclDefinitionException;
import com.devshawn.kafka.gitops.exception.ValidationException;
import com.devshawn.kafka.gitops.util.HelperUtil;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.inferred.freebuilder.FreeBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.common.resource.PatternType;

@FreeBuilder
@JsonDeserialize(builder = ApplicationService.Builder.class)
public abstract class ApplicationService extends ServiceDetails {

    public abstract Optional<String> getPrincipal();

    @JsonProperty("group-id")
    public abstract Optional<String> getGroupId();

    @JsonProperty("pattern")
    public abstract Optional<String> getPattern();

    public abstract List<String> getProduces();

    public abstract List<String> getConsumes();

    @Override
    public List<AclDetails.Builder> getAcls(GetAclOptions options) {
        List<AclDetails.Builder> acls = new ArrayList<>();
        getProduces().forEach(topic -> acls.add(generateWriteACL(topic, getPrincipal())));
        getConsumes().forEach(topic -> acls.add(generateReadAcl(topic, getPrincipal())));

        if (options.getDescribeAclEnabled()) {
            List<String> allTopics = HelperUtil.uniqueCombine(getConsumes(), getProduces());
            allTopics.forEach(topic -> acls.add(generateDescribeAcl(topic, getPrincipal())));
        }

        if (!getConsumes().isEmpty()) {
            String groupId = getGroupId().isPresent() ? getGroupId().get() : options.getServiceName();
            String pattern = getPattern().isPresent() ? getPattern().get() : "LITERAL";
            acls.add(generateConsumerGroupAcl(groupId, getPrincipal(), "READ", pattern));
        }
        return acls;
    }

    @Override
    public void validate() {
        if (getPattern().isPresent()) {
            validateEnum(PatternType.class, getPattern().get(), "pattern");
        }
    }

    private <E extends Enum<E>> void validateEnum(Class<E> enumData, String value, String field) {
        List<String> allowedValues = Arrays.stream(enumData.getEnumConstants()).map(Enum::name).filter(it -> !it.equals("ANY") && !it.equals("UNKNOWN")).collect(Collectors.toList());
        if (!allowedValues.contains(value)) {
            throw new ValidationException(String.format("Invalid value '%s' for %s: allowed values are: %s", value, field, String.join(", ", allowedValues)));
        }
    }

    public static class Builder extends ApplicationService_Builder {

    }
}
