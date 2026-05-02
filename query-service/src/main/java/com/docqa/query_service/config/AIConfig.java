package com.docqa.query_service.config;

import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.embedding.EmbeddingModel;
import org.springframework.ai.google.genai.GoogleGenAiChatModel;
import org.springframework.ai.google.genai.text.GoogleGenAiTextEmbeddingModel;
import org.springframework.ai.openai.OpenAiChatModel;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class AIConfig {

    @Bean("geminiClient")
    @Primary
    public ChatClient geminiClient(GoogleGenAiChatModel model){
        return ChatClient.builder(model).build();
    }

    @Bean("deepseekClient")
    public ChatClient deepSeekClient(OpenAiChatModel model){
        return ChatClient.builder(model).build();
    }

    @Bean
    @Primary
    public EmbeddingModel embeddingModel(GoogleGenAiTextEmbeddingModel model) {
        return model;
    }
}
