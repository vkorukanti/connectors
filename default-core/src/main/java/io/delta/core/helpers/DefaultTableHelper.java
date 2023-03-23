package io.delta.core.helpers;


import java.lang.reflect.Type;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class DefaultTableHelper implements TableHelper {


    /**
     * See https://www.baeldung.com/java-super-type-tokens.
     */
    @Override
    public <T> T fromJson(String json, io.delta.core.helpers.TypeReference<T> coreTypeReference) {
        final com.fasterxml.jackson.core.type.TypeReference<T> jacksonTypeReference =
            new com.fasterxml.jackson.core.type.TypeReference<T>() {
                @Override
                public Type getType() {
                    return coreTypeReference.getType();
                }
            };

        try {
            return new ObjectMapper().readValue(json, jacksonTypeReference);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
