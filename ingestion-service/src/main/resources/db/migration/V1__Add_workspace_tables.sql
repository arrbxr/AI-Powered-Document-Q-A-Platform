-- 1. Create the new workspaces table
CREATE TABLE workspaces (
                            workspace_id VARCHAR(255) PRIMARY KEY,
                            name VARCHAR(255) NOT NULL,
                            created_at TIMESTAMP
);

-- 2. Add the workspace_id column to the existing document_metadata table
-- ⚠️ THE TRAP: Kyunki tumhari table me purane documents ho sakte hain,
-- hum seedha NOT NULL nahi laga sakte warna error aayega.
-- Hum isko pehle add karenge, default value denge, aur phir zaroorat padi toh NOT NULL lagayenge.

ALTER TABLE document_metadata
    ADD COLUMN workspace_id VARCHAR(255) DEFAULT 'WS-DEFAULT';

-- (Optional) Add a foreign key constraint to link documents to workspaces
-- ALTER TABLE document_metadata
-- ADD CONSTRAINT fk_workspace FOREIGN KEY (workspace_id) REFERENCES workspaces(workspace_id);