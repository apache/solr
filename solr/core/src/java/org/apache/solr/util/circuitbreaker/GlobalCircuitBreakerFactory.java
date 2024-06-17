package org.apache.solr.util.circuitbreaker;

import org.apache.solr.core.CoreContainer;

public class GlobalCircuitBreakerFactory {
    private final CoreContainer coreContainerOptional;

    public GlobalCircuitBreakerFactory(CoreContainer coreContainerOptional) {
        this.coreContainerOptional = coreContainerOptional;
    }

    public CircuitBreaker create(String className) throws Exception {
        Class<?> clazz = Class.forName(className);

        if (CircuitBreaker.class.isAssignableFrom(clazz)) {
            Class<? extends CircuitBreaker> breakerClass = clazz.asSubclass(CircuitBreaker.class);

            try {
                // try with 0 arg constructor
                return breakerClass.getDeclaredConstructor().newInstance();
            } catch (NoSuchMethodException e) {
                // otherwise, the CircuitBreaker requires a CoreContainer, so let's give it one
                if (coreContainerOptional != null) {
                    return breakerClass
                            .getDeclaredConstructor(CoreContainer.class)
                            .newInstance(coreContainerOptional);
                } else {
                    throw new IllegalArgumentException(
                            "The CircuitBreaker subclass requires a CoreContainer, but it was not provided");
                }
            }
        } else {
            throw new IllegalArgumentException(
                    "Class " + className + " is not a subclass of CircuitBreaker");
        }
    }
}