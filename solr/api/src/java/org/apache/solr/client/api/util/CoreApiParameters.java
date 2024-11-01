package org.apache.solr.client.api.util;

import static org.apache.solr.client.api.util.Constants.CORE_NAME_PATH_PARAMETER;

import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.enums.ParameterIn;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.METHOD, ElementType.TYPE, ElementType.PARAMETER, ElementType.FIELD})
@Retention(RetentionPolicy.RUNTIME)
@Parameter(name = CORE_NAME_PATH_PARAMETER, in = ParameterIn.PATH)
public @interface CoreApiParameters {}
