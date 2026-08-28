/*
 * Copyright the event-outboxer authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 */
package io.github.bams22.outboxer.spring.serializer;

import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.Message;
import io.github.bams22.outboxer.serializer.protobuf.ProtobufEventSerializer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Autoconfiguration for the additive Protobuf {@code EventSerializer} (ADR-0026). Unlike the
 * Jackson autoconfiguration this bean is deliberately NOT named {@code outboxEventSerializer}:
 * protobuf registers as an extra serializer, so ADR-0025 write-resolution rule 3 keeps the
 * Jackson-named bean as the default writer. Set {@code
 * event-outboxer.serializer.write-format=protobuf} to write protobuf; in protobuf-only setups (the
 * Jackson module excluded from the starter) the single bean writes with zero config (rule 2).
 *
 * <p>Backs off when the user registers their own {@link ProtobufEventSerializer} bean (e.g. with a
 * populated {@link ExtensionRegistryLite}) — two beans with the same {@code format()} id would fail
 * registry construction at startup.
 */
@AutoConfiguration
@ConditionalOnClass({ProtobufEventSerializer.class, Message.class})
public class ProtobufSerializerAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(ProtobufEventSerializer.class)
    public ProtobufEventSerializer outboxProtobufEventSerializer(
            ObjectProvider<ExtensionRegistryLite> extensionRegistry) {
        return new ProtobufEventSerializer(
                extensionRegistry.getIfAvailable(ExtensionRegistryLite::getEmptyRegistry));
    }
}
