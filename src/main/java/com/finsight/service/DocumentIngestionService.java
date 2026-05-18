package com.finsight.service;

import com.finsight.model.Document;
import com.finsight.model.DocumentChunk;
import com.finsight.model.User;
import com.finsight.repository.DocumentChunkRepository;
import com.finsight.repository.DocumentRepository;
import io.qdrant.client.QdrantClient;
import io.qdrant.client.grpc.JsonWithInt;
import io.qdrant.client.grpc.Points;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class DocumentIngestionService {

    private final DocumentRepository documentRepository;
    private final DocumentChunkRepository documentChunkRepository;
    private final StorageService storageService;
    private final PdfExtractionService pdfExtractionService;
    private final EmbeddingService embeddingService;
    private final QdrantClient qdrantClient;

    @Value("${qdrant.collection-name}")
    private String collectionName;

    private static final int CHUNK_SIZE = 500;
    private static final int CHUNK_OVERLAP = 50;

    @Transactional
    public Document initiateIngestion(MultipartFile file, User user,
                                      Document.DocumentType documentType,
                                      String companyName, Integer fiscalYear) {
        Document document = Document.builder()
                .user(user)
                .originalName(file.getOriginalFilename())
                .storedFilename(UUID.randomUUID().toString())
                .fileSizeBytes(file.getSize())
                .documentType(documentType)
                .companyName(companyName)
                .fiscalYear(fiscalYear)
                .status(Document.Status.PENDING)
                .chunkCount(0)
                .build();

        document = documentRepository.save(document);
        log.info("Created document record with id: {}", document.getId());

        processDocumentAsync(document.getId(), file, user.getId());

        return document;
    }

    @Async
    public void processDocumentAsync(Long documentId, MultipartFile file, Long userId) {
        Document document = documentRepository.findById(documentId)
                .orElseThrow(() -> new RuntimeException("Document not found: " + documentId));

        try {
            document.setStatus(Document.Status.PROCESSING);
            documentRepository.save(document);
            log.info("Processing document: {}", document.getOriginalName());

            String minioKey = storageService.uploadFile(file, userId);
            document.setMinioKey(minioKey);
            documentRepository.save(document);
            log.info("Uploaded to MinIO: {}", minioKey);

            List<PdfExtractionService.PageContent> pages =
                    pdfExtractionService.extractPages(file.getInputStream());
            document.setPageCount(pages.size());
            log.info("Extracted {} pages from document", pages.size());

            List<ChunkData> chunks = chunkPages(pages);
            log.info("Created {} chunks from document", chunks.size());

            List<DocumentChunk> savedChunks = new ArrayList<>();
            List<Points.PointStruct> qdrantPoints = new ArrayList<>();

            for (int i = 0; i < chunks.size(); i++) {
                ChunkData chunkData = chunks.get(i);

                List<Float> vector = embeddingService.embedText(chunkData.text());
                String qdrantId = UUID.randomUUID().toString();

                Points.PointStruct point = Points.PointStruct.newBuilder()
                        .setId(Points.PointId.newBuilder()
                                .setUuid(qdrantId)
                                .build())
                        .setVectors(Points.Vectors.newBuilder()
                                .setVector(Points.Vector.newBuilder()
                                        .addAllData(vector)
                                        .build())
                                .build())
                        .putPayload("document_id",
                                JsonWithInt.Value.newBuilder()
                                        .setIntegerValue(documentId)
                                        .build())
                        .putPayload("user_id",
                                JsonWithInt.Value.newBuilder()
                                        .setIntegerValue(userId)
                                        .build())
                        .putPayload("page_number",
                                JsonWithInt.Value.newBuilder()
                                        .setIntegerValue(chunkData.pageNumber())
                                        .build())
                        .putPayload("chunk_index",
                                JsonWithInt.Value.newBuilder()
                                        .setIntegerValue(i)
                                        .build())
                        .putPayload("chunk_text",
                                JsonWithInt.Value.newBuilder()
                                        .setStringValue(chunkData.text())
                                        .build())
                        .build();

                qdrantPoints.add(point);

                DocumentChunk chunk = DocumentChunk.builder()
                        .document(document)
                        .chunkIndex(i)
                        .chunkText(chunkData.text())
                        .pageNumber(chunkData.pageNumber())
                        .charOffset(chunkData.charOffset())
                        .tokenCount(chunkData.text().split("\\s+").length)
                        .qdrantId(qdrantId)
                        .build();

                savedChunks.add(chunk);
                log.debug("Embedded chunk {}/{}", i + 1, chunks.size());
            }

            qdrantClient.upsertAsync(collectionName,
                    qdrantPoints).get();
            log.info("Stored {} vectors in Qdrant", qdrantPoints.size());

            documentChunkRepository.saveAll(savedChunks);
            log.info("Saved {} chunks to MySQL", savedChunks.size());

            document.setChunkCount(savedChunks.size());
            document.setStatus(Document.Status.PROCESSED);
            documentRepository.save(document);
            log.info("Document {} processed successfully with {} chunks",
                    document.getOriginalName(), savedChunks.size());

        } catch (Exception e) {
            log.error("Failed to process document {}: {}", documentId, e.getMessage(), e);
            document.setStatus(Document.Status.FAILED);
            documentRepository.save(document);
        }
    }

    private List<ChunkData> chunkPages(List<PdfExtractionService.PageContent> pages) {
        List<ChunkData> chunks = new ArrayList<>();

        for (PdfExtractionService.PageContent page : pages) {
            String text = page.text();
            String[] words = text.split("\\s+");
            int start = 0;

            while (start < words.length) {
                int end = Math.min(start + CHUNK_SIZE, words.length);
                StringBuilder chunkText = new StringBuilder();

                for (int i = start; i < end; i++) {
                    if (i > start) chunkText.append(" ");
                    chunkText.append(words[i]);
                }

                chunks.add(new ChunkData(
                        chunkText.toString(),
                        page.pageNumber(),
                        start
                ));

                start += CHUNK_SIZE - CHUNK_OVERLAP;
            }
        }

        return chunks;
    }

    private record ChunkData(String text, int pageNumber, int charOffset) {}
}