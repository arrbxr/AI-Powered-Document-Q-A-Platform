# AI-Powered Document Q&A Platform

A modern microservices-based platform that enables users to upload PDF and document files and ask natural language questions about their content. The system uses AI models (Google Gemini, OpenAI, Groq) with vector embeddings to provide intelligent, context-aware answers.

## 🚀 Features

- **Document Upload**: Upload PDF and document files securely to cloud storage (MinIO)
- **AI-Powered Q&A**: Ask natural language questions about your documents
- **Vector Embeddings**: Semantic search using vector databases for accurate context retrieval
- **Multi-AI Support**: Seamlessly switch between Google Gemini, OpenAI, and Groq models
- **Chat History**: Maintains conversation history for follow-up questions
- **Microservices Architecture**: Scalable, independent services for document ingestion and query processing
- **Async Processing**: Uses Kafka for asynchronous document processing
- **Production-Ready**: Docker Compose setup with complete infrastructure

## 📋 System Architecture

<img width="968" height="840" alt="image" src="https://github.com/user-attachments/assets/c3b88d60-d330-49b8-98f1-c21f1f240dde" />


## 🛠️ Tech Stack

### Backend
- **Java 21**: Latest LTS version
- **Spring Boot 3.5.13 & 4.0.5**: Modern framework
- **Spring AI**: Unified AI framework with LLM support
- **Spring Data JPA**: ORM for database operations
- **Spring Kafka**: Asynchronous messaging

### Infrastructure
- **PostgreSQL + pgvector**: Vector database for embeddings
- **Apache Kafka (KRaft)**: Event streaming platform
- **MinIO**: S3-compatible object storage
- **Docker**: Containerization

### AI Models
- **Google Gemini**: Primary LLM
- **OpenAI GPT**: Fallback LLM
- **Groq**: Fast LLM inference
- **Google GenAI Embeddings**: Vector embeddings

### Libraries
- **Resilience4j**: Circuit breaker pattern
- **Lombok**: Code generation
- **OkHttp**: HTTP client
- **Minio Java SDK**: Object storage client

## 📦 Prerequisites

- **Docker & Docker Compose** (v20.10+)
- **Java 21** (for local development)
- **Maven 3.8+** (for local building)
- **API Keys**:
  - Google Gemini API key
  - OpenAI API key (optional)
  - Groq API key (optional)

## 🚀 Quick Start

### 1. Clone the Repository
```bash
git clone https://github.com/arrbxr/AI-Powered-Document-Q-A-Platform.git
cd AI-Powered-Document-Q-A-Platform


